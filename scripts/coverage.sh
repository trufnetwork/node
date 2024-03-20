go test -coverprofile='coverage.out' ./internal/extensions/...
go tool cover -html='coverage.out' -o 'coverage.html'
if grep -qi microsoft /proc/version; then
	explorer.exe coverage.html
else
	open coverage.html
fi
sleep 1
rm -rf coverage.out coverage.html
