del /f blobhandler.exe
cargo build --release
move  target\release\blobhandler.exe blobhandler.exe
func start