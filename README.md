## Mongoimport

```bash
go run github.com/romnnn/mongoimport/cmd/mongoimport csv <path-to-csv>
```

#### TODOS
- JSON and XML Loaders
- Update logic (fallback to updateOne in batch iteration)
- Safe deletion as a real pre processing step in parallel
- Some tests
- Some cli
- travis
- Some examples