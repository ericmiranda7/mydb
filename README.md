# mydb
Working through implementing a db (for now key-val) for the funs

# Implementations
### Simplest
Use a log and append every key value to the end of the file. Read from the file by scanning through
all the keys.

**Pros**
Super fast writes

**Cons**
- O(n) reads
- Disk space of overwritten keys are never reclaimed

### Naive byte offset
The simplest idea is we use (a very) basic index. Key is the key being inserted into the database
while value is byte offset of that key in the dbfile.

**Pros** O(n) read is now O(1)

**Cons**
- Writes have become more complicated (as we have to maintain an index)
- Index as metadata increases storage size
- Overwritten keys still aren't reclaimed
- On restarts, we need to scan the entire log to rebuild the index which is very expensive
- No. of keys is the amount of RAM you have
- Unable to make range queries efficiently (give me all keys / values between 'apple' and 'bat')

#### Byte Offset + Merging & Compaction
**Cons**
- [ ] Writes have become more complicated (as we have to maintain an index)
- [ ] Index as metadata increases storage size
- [x] Overwritten keys still aren't reclaimed
- [x] On restarts, we need to scan _smaller log segments to rebuild the index which is not too expensive_
- [ ] No. of keys is the amount of RAM you have
- [ ] Unable to make range queries efficiently (give me all keys / values between 'apple' and 'bat')


### SSTable Format
We can solve a few of the cons:
**Cons**
- [ ]Writes have become more complicated (as we have to maintain an index)
- [ ] Index as metadata increases storage size
- [x] Overwritten keys still aren't reclaimed
- [x] On restarts, we need to scan the entire log to rebuild the index which is very expensive
- [x] No. of keys is the amount of RAM you have
- [x] Unable to make range queries efficiently (give me all keys / values between 'apple' and 'bat')

Using an SSTable!