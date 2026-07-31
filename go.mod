module akita-cache-project

go 1.24.0

require github.com/sarchlab/akita/v4 v4.9.0

replace github.com/sarchlab/akita/v4 => ./akita

require (
	github.com/mattn/go-sqlite3 v1.14.32 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/tebeka/atexit v0.3.0 // indirect
)
