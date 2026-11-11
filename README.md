# Sync.NET — Phase 0

Kiểm chứng ngày 2026-11-10 trên Windows x64 với .NET SDK 10.0.402, runtime 10.0.13 và Node 24.19.0 x64. Phạm vi lấy từ báo cáo Sync.NET-FSA-Sync-Engine-Analysis.md, phân tích FSA tại commit `9c0407ea9d2a40496d54a07cce4949000fe9029d`. Giữ bề mặt SDK độc lập với miền nghiệp vụ và ưu tiên kiểm chứng cơ chế của KineDB trước khi thiết kế journal hoặc giao thức đồng bộ.

| Hạng mục | Kết quả | Bằng chứng |
| --- | --- | --- |
| Build solution .NET 10 | PASS | 0 warning, 0 error |
| Offline CRUD | PASS | .NET gọi KineDB Wasm qua tiến trình Node. Kiểm tra giá trị đọc được sau insert, update và delete. Không cấu hình server. |
| Dữ liệu sau khi khởi động lại | PASS | Tiến trình Node đầu tiên đóng database và thoát. Tiến trình Node mới mở cùng thư mục và xác nhận giá trị đã cập nhật cùng việc bản ghi đã xóa vẫn vắng mặt. |
| Integration test | PASS | xUnit chạy 1 test thật với gói đã pin. Khi thiếu biến môi trường, test báo skip kèm lý do BLOCKED. |
| Native embedded trong .NET Windows x64 | BLOCKED | Không có DLL/header C ABI Windows x64 đã xác minh. Kho source yêu cầu xác thực khi thử `git ls-remote` ẩn danh. Archive Windows MSVC cung cấp server EXE, nhưng không kèm DLL hay header. Ba tên asset thử trực tiếp ở registry cùng commit (`libkinedb_wasm.dll`, `kinedb_wasm.dll`, `kinedb.h`) trả HTTP 404. Tài liệu Python chỉ liệt kê native wheel Linux x64 và macOS arm64. |
| Điều khoản tích hợp và phân phối | BLOCKED | Manifest gói Wasm khai báo `UNLICENSED` và tarball không có LICENSE riêng. Chưa xác nhận điều khoản cho native SDK/server hoặc quyền đóng gói lại. Repo chỉ chứa mã probe. |
| Server native Windows x64 | PASS | Đã chạy đúng commit trên loopback, kiểm tra HTTP health và truy vấn SQL có xác thực. |
| Hai replica Wasm, push/pull và conflict | PASS | Push 3 thay đổi offline sau khi mở lại, pull delta 2 upserts và 1 delete. Push hỗn hợp gửi 2 thao tác, trả 1 applied và 1 conflict. Refusal giữ nguyên sau reopen, push kế tiếp gửi 0 thao tác. |
| Push giao dịch trên một bảng | PASS | Khi một thao tác xung đột, cả nhóm 2 thao tác bị từ chối. Nhóm hợp lệ ghi cả 2 hàng cùng version. |
| Push giao dịch trên hai bảng | PASS | Push từ một bảng gửi cả nhóm. Nhóm xung đột không ghi hàng ở bảng nào. Nhóm hợp lệ ghi hai hàng cùng version. |
| Giao dịch cha–con có khóa ngoại | PASS | Push từ bảng con gửi cả cha lẫn con. Nếu cha bị xóa trên server trước push, cả nhóm bị từ chối với `fk_parent_missing`. |
| Checkpoint khi pull bị ngắt | PASS | Chặn frame dữ liệu, mở lại replica thấy dữ liệu và token cũ. Pull tiếp từ token cũ nhận 1 upsert và 1 delete. Dữ liệu mới và token mới còn sau reopen. |
| Checkpoint sau khi nhận một phần live stream | PASS | Áp dụng frame đầu, chặn frame kế tiếp. Reopen giữ dữ liệu và token của frame đầu. Pull tiếp nhận đúng thay đổi còn thiếu. |
| Delta có bộ lọc | PASS | Một hàng vào và một hàng rời bộ lọc. Delta báo 1 upsert, 1 delete và cache sau reopen chỉ còn hàng đúng phạm vi. |
| Pull one-shot nhiều trang | PASS | Chặn trang thứ hai, mở lại rồi pull đủ 1.200 hàng và xác nhận checkpoint cuối. |
| Checkpoint hết hạn | PASS | Server retention riêng từ chối truy vấn commit cũ. Replica phục hồi hàng hiện tại, bỏ hàng cũ và lưu checkpoint mới. |
| Replay qua lần khởi động lại server | PASS | Kill replica sau khi chặn ACK, xác nhận hàng trên server, buộc dừng server rồi mở lại cùng data dir. Retry trả replayed và push tiếp gửi 0 thao tác. |
| Replay sau mất phản hồi | PASS | Chặn phản hồi WebSocket, kill tiến trình gửi push khi database còn mở, xác nhận server đã commit rồi mở replica trong tiến trình khác để retry. Outcome báo replayed và không phát sinh delta mới ở mốc retry mặc định và sau 60 giây. |

Console offline kiểm tra đóng tiến trình có kiểm soát với Wasm `durability: 'commit'`. Probe đồng bộ riêng đã kiểm tra replay khi replica bị kill và khi server bị buộc dừng. Chưa thử mất điện hoặc lỗi đĩa. `ILocalSql` là interface thử nghiệm cho một batch SQL, với mỗi lệnh commit riêng. Console và test dùng cùng kịch bản. Probe không có hợp đồng sync hoặc outbox riêng.

## Bản dựng KineDB

- Commit server từ `REVISION.txt` là `3c2ace4d54857df44bf160fb71ddb2cb484d1009`.
- Wasm dùng `@kinedb/embedded@0.0.0-g3c2ace4d5485`. SHA-256 của tarball là `A85AD580BA798344F487210DFE9667F1A93B9EA61D1898EEC8397903CA98D6BC`. Harness kiểm tra phiên bản gói và SHA-256 của module Wasm trước khi chạy. [Registry KineDB](https://git.kinedb.com/kinedb/-/packages)
- Archive server Linux x64 musl dùng SHA-256 `C6C7363F5131E05401F8D64FEAA22786C03DDCF9910ACB9A7A426DE31321C479`, trùng checksum do KineDB công bố. [Archive server](https://git.kinedb.com/api/packages/kinedb/generic/kinedb/3c2ace4d5485/kinedb-x86_64-unknown-linux-musl.tar.gz) · [Checksum](https://git.kinedb.com/api/packages/kinedb/generic/kinedb/3c2ace4d5485/SHA256SUMS-linux.txt)
- Đối chiếu nền tảng và yêu cầu khớp bản dựng qua tài liệu nhà cung cấp. [Embedded Wasm](https://docs.kinedb.com/clients/embedded.html) · [Python native](https://docs.kinedb.com/clients/python#the-library-file) · [Server](https://docs.kinedb.com/run)

## Tái lập từ PowerShell

```powershell
Set-Location 'D:\Projects\Code Samples\Sync.NET'
$phaseDir = (New-Item -ItemType Directory -Force .phase0).FullName
$packageArchive = Join-Path $phaseDir 'kinedb-embedded-0.0.0-g3c2ace4d5485.tgz'
npm.cmd pack @kinedb/embedded@0.0.0-g3c2ace4d5485 --registry=https://git.kinedb.com/api/packages/kinedb/npm/ --pack-destination $phaseDir
if ($LASTEXITCODE) { throw 'KineDB download failed' }
if ((Get-FileHash $packageArchive).Hash -ne 'A85AD580BA798344F487210DFE9667F1A93B9EA61D1898EEC8397903CA98D6BC') {
    throw 'KineDB archive checksum mismatch'
}
tar -xf $packageArchive -C $phaseDir
if ($LASTEXITCODE) { throw 'KineDB extraction failed' }
$env:SYNCNET_KINEDB_WASM_PACKAGE = Join-Path $phaseDir 'package'

dotnet build Sync.NET.slnx
dotnet run --project tools/Sync.NET.Feasibility --no-build -- $env:SYNCNET_KINEDB_WASM_PACKAGE
dotnet test Sync.NET.slnx --no-build --logger 'console;verbosity=normal'
```

Console trả 0 khi probe Wasm pass, 1 khi probe fail và 2 khi thiếu đối số hoặc thiếu tệp KineDB. Console báo riêng trạng thái native và phạm vi kiểm tra đồng bộ. Dữ liệu probe nằm trong thư mục tạm riêng và được dọn sau khi chạy.

Để tái lập kiểm tra quyền đọc source ẩn danh, đặt `$env:GIT_TERMINAL_PROMPT = '0'` rồi chạy `git -c credential.helper= ls-remote https://git.kinedb.com/kinedb/kinedb.git HEAD`. Để đối chiếu archive server đã tải, chạy `Get-FileHash` trên archive và `tar -xOf kinedb-x86_64-unknown-linux-musl.tar.gz kinedb-x86_64-unknown-linux-musl/REVISION.txt`.


## Server KineDB native trên Windows x64

Archive Windows MSVC dạng `.tar.gz` cùng commit đã được tìm thấy và xác minh. SHA-256 là `213d87c30330e816e0b43495fd836c528ff79a245ad78770b312f3590eb2f0e6`. `kinedb.exe --version` in đúng commit đã pin. [Archive Windows](https://git.kinedb.com/api/packages/kinedb/generic/kinedb/3c2ace4d5485/kinedb-x86_64-pc-windows-msvc.tar.gz) · [Checksum](https://git.kinedb.com/api/packages/kinedb/generic/kinedb/3c2ace4d5485/SHA256SUMS-windows.txt)

Server test chạy dưới dạng tiến trình cục bộ tại `127.0.0.1:4820`. `GET /health` trả HTTP 200 và SQL shell xác thực thành công rồi trả 1 cho `SELECT 1 AS phase0`. Binary, dữ liệu, log và mật khẩu root được lưu dưới `.phase0`. Mật khẩu được bảo vệ theo tài khoản Windows hiện tại.

Sau khi dừng tiến trình cũ, có thể khởi động lại từ thư mục repo bằng PowerShell.

```powershell
Set-Location .phase0
$env:KINEDB_ROOT_PASSWORD = (Import-Clixml .\windows-server-root.xml).GetNetworkCredential().Password
try {
    .\kinedb-x86_64-pc-windows-msvc\kinedb.exe --listen 127.0.0.1 --port 4820 --data-dir windows-server-data --no-web
} finally {
    Remove-Item Env:KINEDB_ROOT_PASSWORD
}
```


### Tái lập kiểm thử hai replica

Sau khi server Windows đã chạy và đã giải nén gói Wasm theo hướng dẫn trên, chạy từ thư mục repo.

```powershell
$env:KINEDB_ROOT_PASSWORD = (Import-Clixml .phase0/windows-server-root.xml).GetNetworkCredential().Password
try {
    node tools/Sync.NET.Feasibility/kinedb-sync-probe.mjs $env:SYNCNET_KINEDB_WASM_PACKAGE
    if ($LASTEXITCODE) { throw 'KineDB sync probe failed' }
} finally {
    Remove-Item Env:KINEDB_ROOT_PASSWORD
}
```

Probe dùng bảng tên ngẫu nhiên, hai thư mục replica và dữ liệu tổng hợp. Kịch bản kiểm tra initial pull, ba thay đổi offline còn trong journal sau khi mở lại, push và delta pull. Khi hai replica sửa cùng bản ghi, replica thứ hai push cả bản sửa stale lẫn một insert độc lập. Server giữ giá trị thắng và ghi hàng mới. Report trả riêng `conflict` và `applied`. Sau reopen, `refused(table, session)` trả đúng mục đã lưu và push kế tiếp gửi 0 thao tác. Gọi `refused(table)` ngay sau reopen, khi chưa có session, báo lỗi thiếu thông tin kết nối. Kịch bản replay chặn phản hồi WebSocket ở phía client sau khi gửi một insert, đợi worker xác nhận đã chặn ACK, kill worker bằng `SIGKILL` khi database còn mở, xác nhận hàng đã xuất hiện trên server, rồi mở replica trong tiến trình khác. Lần retry báo `applied` với `replayed: true`, lần pull kế tiếp có 0 upserts và 0 deletes.

Probe cũng gửi hai giao dịch trên cùng bảng. Với giao dịch có xung đột, server trả `conflict` cho bản sửa và `rejected` cho insert đi cùng, không ghi hàng nào trong nhóm. Cả hai mục xuất hiện trong `refused()`. Với giao dịch hợp lệ, cả hai insert được áp dụng cùng version.

Với hai bảng đã sync có khóa ngoại, push từ bảng cha gửi cả nhóm có bản sửa stale và server từ chối cả hai thao tác. Nhóm hợp lệ tạo cha trước, rồi tạo con trong cùng giao dịch offline. Push từ bảng con áp dụng cả hai cùng version, và push tiếp từ bảng cha gửi 0 thao tác. Khi cha đã tồn tại lúc replica ghi offline nhưng bị xóa trên server trước push, server từ chối cả nhóm. `refused()` của bảng con ghi `fk_parent_missing`, và insert độc lập đi cùng nhóm cũng không xuất hiện trên server.

Probe checkpoint ngắt WebSocket khi frame delta chứa giá trị đánh dấu đến, trước khi chuyển frame vào Wasm. Replica mở lại giữ nguyên dữ liệu và token cũ. Lần sync tiếp dùng chính token đó, nhận đủ 1 upsert và 1 delete, không cần full fill. Mở lại lần nữa xác nhận cả token mới và dữ liệu mới. Ca này dùng delta một frame và chưa bao phủ gián đoạn sau khi đã áp dụng một phần của pull nhiều frame.

Ca live stream nhận một frame cập nhật, kiểm tra token đã tiến, rồi chặn frame chứa thay đổi kế tiếp. Sau reopen, replica giữ hàng và token của frame đã áp dụng. Pull một lần nhận đúng thay đổi còn thiếu. Đây là hai frame trên một live subscription; chưa thử gián đoạn giữa nhiều frame của một pull one-shot.

Ca lọc dùng replica riêng với điều kiện `payload = 'in_scope'`. Server đổi một hàng vào và một hàng ra khỏi phạm vi. Delta tiếp tục từ token cũ, báo 1 upsert và 1 delete. Replica mở lại chỉ chứa hàng vừa vào phạm vi.

Các replica chạy qua Node/Wasm. Probe sync kiểm tra việc tiến trình Node bị kill trước khi tiến trình khác retry. Chưa thử mất điện hay replay sau khi hết thời hạn lưu kết quả. Integration test xUnit riêng vẫn bao phủ việc thoát và khởi động tiến trình mới cho offline CRUD.


## Chạy toàn bộ Phase 0 một lần

Từ thư mục repo, khởi động server Windows chính ở cổng 4820 và giải nén gói Wasm đã pin theo hướng dẫn trên. Sau đó chạy:

```powershell
.\tools\Sync.NET.Feasibility\run-phase0.ps1
```

Có thể truyền `-PackageDirectory` nếu gói nằm ngoài `.phase0/package`. Lệnh chạy build, console offline, xUnit, probe đồng bộ, probe pull nhiều trang và probe checkpoint hết hạn. Probe retention khởi động server riêng với data dir và log riêng trong `.phase0`, cổng mặc định 4821 và `--gc-retention 0commits`. Server riêng tự dừng sau lượt chạy. Chọn cổng khác với `-RetentionPort` nếu cần. [Cấu hình retention của KineDB](https://docs.kinedb.com/run#command-line-arguments)

- **Pull nhiều trang:** đưa khoảng 9,4 MiB vào một bảng mới, chuyển trang dữ liệu đầu tiên vào Wasm và chặn trang thứ hai. Mở lại replica, kiểm tra toàn bộ 1.200 hàng sau khi pull lại và checkpoint cuối sau khi mở lại lần nữa. [Phân trang WATCH](https://docs.kinedb.com/sql#watch-live-queries)
- **Checkpoint hết hạn:** lưu checkpoint cũ, tạo trạng thái server mới, rồi đợi truy vấn lịch sử xác nhận commit cũ đã vượt retention. Kiểm tra cache sau phục hồi có hàng hiện tại và đã bỏ hàng cũ. [Khoảng trống khi resume](https://docs.kinedb.com/sql#limits-and-the-gap-answer-allow-gap-and-max-resume-rows)
- **Replay có trì hoãn:** thêm `-ReplayDelaySeconds <số-giây>` vào cùng lệnh để đo replay ở một mốc cụ thể sau khi server đã ghi và worker bị kill. Kết quả pass tại mốc đó chỉ cho biết replay còn hoạt động sau khoảng chờ đã chọn. Chưa có thiết lập đã xác minh để ép kết quả replay hết hạn. Retention của lịch sử pull được kiểm tra riêng.

Trình chạy gộp đã hoàn tất với mã thoát 0 trên Windows x64. Lượt gộp dùng độ trễ replay mặc định 0 giây. Lượt probe đồng bộ riêng với `SYNCNET_KINEDB_REPLAY_DELAY_SECONDS=60` cũng pass. Retry báo `applied` và `replayed: true`, rồi pull nhận 0 upserts và 0 deletes. Kết quả chỉ xác nhận mốc 60 giây trên server đang chạy. Chưa đo giới hạn giữ replay. Probe server restart riêng đã pass trên cùng bản dựng.

## Replay sau khi khởi động lại server

Chạy riêng từ thư mục repo bằng PowerShell.

```powershell
.\tools\Sync.NET.Feasibility\test-server-restart.ps1 -PackageDirectory '<thư-mục-gói-Wasm>'
```

Probe tạo server riêng trên cổng 4821, dùng data dir ngẫu nhiên trong `.phase0`, chặn ACK và kill tiến trình replica. Sau khi xác nhận hàng đã có trên server, probe buộc dừng server và mở lại cùng data dir. Replica retry trả `applied` với `replayed: true`, hàng vẫn tồn tại và lần push tiếp gửi 0 thao tác. Server tạm tự dừng sau probe, data dir và log được giữ trong `.phase0`.

Probe riêng đã pass. Build và test CRUD cũng pass. Lệnh chạy gộp đã thêm probe này nhưng lượt kiểm chứng gộp mới bị cơ chế xét duyệt tự động chặn khi điều phối khởi động server chính, với lý do `blocked by policy`.

## Bước tiếp theo

1. Lấy từ KineDB DLL Windows x64 và header C ABI có local, remote và sync exports, hoặc quyền truy cập source và quy trình build đúng commit. Xác nhận điều khoản tích hợp và phân phối. Chạy open/execute/close từ .NET bằng DLL rồi lặp lại probe ở ranh giới tiến trình và sau kill.
2. Lặp lại probe sync với DLL native và thử kill tiến trình sau khi mất ACK. Đo replay ở các mốc trì hoãn khi đã xác định yêu cầu giữ kết quả của SDK.

## Phase 1 · Đồng bộ một bảng từ .NET

`Sync.NET` cung cấp `ISyncCollection<T>`, mapping kiểu dữ liệu, tiến độ và kết quả push/pull. Project `Sync.NET.KineDB` triển khai interface qua Node và gói KineDB Wasm đã pin. Nó dùng journal, tombstone và checkpoint của KineDB. SDK chỉ lưu thông tin nhận diện và trạng thái khởi tạo replica. Phần nhúng native và quyền phân phối KineDB vẫn chờ xác minh.

Mỗi thư mục replica chứa một bảng và được gắn với tên bảng, `schemaId`, endpoint và user. Server cần bảng `(id TEXT PRIMARY KEY, payload TEXT NOT NULL)`. Sau lần sync đầu tiên, `GetAsync`, `ListAsync`, `PutAsync` và `DeleteAsync` dùng dữ liệu local. `PutAsync` thay thế toàn bộ payload của cùng khóa. Mỗi lần `SynchronizeAsync` sau đó push trước rồi pull từ checkpoint của KineDB. Kết quả chứa trạng thái, số hàng thay đổi và kết quả từng thao tác. Khi push có kết quả chưa rõ, caller có thể gọi sync lại để KineDB thực hiện replay.

Các lệnh ghi chờ nếu replica đang sync. Trong ca kiểm thử giữ ACK, bản Wasm đã pin đưa giá trị cũ trở lại sau khi một bản sửa mới được ghi giữa push và pull. Vì vậy Phase 1 chỉ báo ghi thành công sau khi lệnh chờ được commit local. Hủy sync giữa push có thể trả `PushOutcomeUnknown` nếu server đã commit nhưng client chưa nhận ACK. Lệnh ghi đã gửi vào worker được chờ đến khi có kết quả local.

### Chạy console mẫu

Khởi động server Windows như hướng dẫn trên. Từ thư mục repo, đặt `$package` là đường dẫn gói Wasm đã giải nén, rồi chạy:

```powershell
$package = (Resolve-Path .phase0/package).Path
$env:KINEDB_ROOT_PASSWORD = (Import-Clixml .phase0/windows-server-root.xml).GetNetworkCredential().Password
try {
    dotnet build Sync.NET.slnx
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-a init
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-a sync
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-b sync
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-a put demo 'Hello offline'
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-a sync
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-b sync
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-b get demo
} finally {
    Remove-Item Env:KINEDB_ROOT_PASSWORD
}
```

`init` tạo bảng mẫu `phase1_notes` nếu chưa có. Lệnh `put` chỉ ghi local. Có thể dùng `list` để đọc replica hoặc `delete <id>` để ghi xóa offline rồi sync như trên.

### Chạy integration tests

```powershell
$env:SYNCNET_KINEDB_WASM_PACKAGE = (Resolve-Path .phase0/package).Path
$env:SYNCNET_KINEDB_SERVER_EXE = (Resolve-Path .phase0/kinedb-x86_64-pc-windows-msvc/kinedb.exe).Path
dotnet test Sync.NET.slnx
```

Các test Phase 1 tự mở server Windows tạm trên loopback. Test chạy bootstrap, insert/update/delete offline, mở lại replica, đối chiếu hai replica, kết quả conflict, lệnh ghi chờ ACK và replay sau khi hủy push giữa chừng. Hai biến môi trường phải trỏ đến asset cùng bản dựng đã pin. Thay đường dẫn `.phase0/package` nếu đã giải nén gói ở chỗ khác.
