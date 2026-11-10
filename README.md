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
| Replay sau mất phản hồi | PASS | Chặn phản hồi WebSocket, kill tiến trình gửi push khi database còn mở, xác nhận server đã commit rồi mở replica trong tiến trình khác để retry. Outcome báo replayed và không phát sinh delta mới. |

Kết quả restart áp dụng cho đóng tiến trình có kiểm soát, với cấu hình Wasm `durability: 'commit'`. Chưa thử kill đột ngột, mất điện, lỗi đĩa hoặc khôi phục pending writes. `ILocalSql` là interface thử nghiệm cho một batch SQL, với mỗi lệnh commit riêng. Console và test dùng cùng kịch bản. Probe không có hợp đồng sync hoặc outbox riêng.

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

Với giao dịch trên hai bảng đã sync, một lần `push()` từ bảng chính gửi cả hai thao tác. Bản sửa stale làm cả nhóm bị từ chối, bảng còn lại không có hàng mới. Với nhóm hợp lệ, mỗi bảng nhận một hàng và cả hai outcome có cùng version. Push tiếp từ bảng còn lại gửi 0 thao tác trong cả hai trường hợp. Kịch bản hai bảng chưa bao phủ ràng buộc khóa ngoại.

Hai replica chạy qua Node/Wasm. Probe sync kiểm tra việc tiến trình Node bị kill trước khi tiến trình khác retry. Chưa thử mất điện hay replay sau khi hết thời hạn lưu kết quả. Integration test xUnit riêng vẫn bao phủ việc thoát và khởi động tiến trình mới cho offline CRUD.

## Bước tiếp theo

1. Lấy từ KineDB DLL Windows x64 và header C ABI có local, remote và sync exports, hoặc quyền truy cập source và quy trình build đúng commit. Xác nhận điều khoản tích hợp và phân phối. Chạy open/execute/close từ .NET bằng DLL rồi lặp lại probe ở ranh giới tiến trình và sau kill.
2. Lặp lại probe sync với DLL native và thử kill tiến trình sau khi mất ACK. Đo thời hạn giữ kết quả replay và kiểm tra thứ tự push và việc từ chối nhóm giao dịch có ràng buộc khóa ngoại trước khi chốt hợp đồng SDK.
