## About

Database implementation for docs.
Uses B+ Trees for implementing a Read oriented database

Adapted from [cute db](https://github.com/naqvijafar91/cuteDB)

> [!IMPORTANT]
> The redis-benchmark test doesn't work due to it missing some append only command. got to work on that

```bash
#To run the client
cd #to the base folder
go run .
```

todos:
- [ ] Write the main server to test out the implementation
- [x] Try testing out with multiple clients
