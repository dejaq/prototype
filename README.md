
### Setup

* Golang 1.13+ https://golang.org/dl/
* Goland (or other IDE) with VGO activated
* FlatBuffer compiler https://rwinslow.com/posts/how-to-install-flatbuffers/

```bash
go install golang.org/x/tools/cmd/stringer

#Install dependencies for all main modules
cd broker/
go mod download
cd ../grpc
go mod download
```