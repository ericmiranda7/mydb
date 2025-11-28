# mydb
Working at Target scale has made me insecure about my skills.  
Dealing with upwards of 160k RPS, I was quickly reminded 
how little I knew about building software at scale.  
So I started reading [DDIA](https://www.google.co.in/books/edition/Designing_Data_Intensive_Applications/zFheDgAAQBAJ?hl=en).  
Reading DDIA, I was quickly reminded how poor I am at understanding abstract concepts[^1].  
So I started implementing a storage engine.

As I work through an implementation, I'll try to list out the pros & cons of the solution.
Computer science is all about tradeoffs and this becomes very evident when by trying to solve a con,
we introduce a bunch more!

> Note: I ignore a lot of details, such as concurrent writes, complex key-value data, etc. to focus
> on the broader algorithmic ideas

# Implementations
### Naive and Simple (How Everyone Sees Me)
Use a log and append every key-value to the end of the file. Read from the file by scanning through
all the keys.

**Pros**
- Superfast writes
- Smooth brain like

**Cons**
1. O(n) reads
2. Disk space of overwritten keys are never reclaimed

### Naive Byte Offset
Let's solve con 1!
The simplest idea is to use a key-value index. Key is the key being inserted into the database
and value is the byte offset of that key in the logfile.

**Pros**
- O(n) read is now O(1)

**Cons**
1. [x] O(n) reads
2. [ ] Disk space of overwritten keys are never reclaimed
3. [ ] Writes have become more complicated (as we have to maintain an index)
4. [ ] Index as metadata increases storage size
5. [ ] On restarts, we need to scan the entire log to rebuild the index which is very expensive
6. [ ] No. of keys is the amount of RAM you have
7. [ ] Unable to make range queries efficiently (give me all keys between 'apple' and 'bat')

Solving a single con resulted in another 5! Tradeoffs, amirite

#### Byte Offset + Merging & Compaction
Let's solve con 2 & 5
Rather than maintaining one massive logfile, let's write the log to disk after every x mb. This is called a segment.
On this segment, we could run
compaction (replacing old key-values with only the latest occurrence). Further, we could instead merge multiple segments
into 1, allowing for compaction across segments, aka merging.

**Cons**
1. [x] Disk space of overwritten keys are never reclaimed
2. [ ] Writes are still complicated
3. [ ] Index as metadata increases storage size
4. [x] On restarts, we need to scan the entire log to rebuild the index which is very expensive (Now we only require
   to scan smaller, compacted segments)
5. [ ] No. of keys is the amount of RAM you have
6. [ ] Unable to make range queries efficiently (give me all keys / values between 'apple' and 'bat')


### TODO(): SSTable Format
Let's solve con 5 & 6, using an SSTable!
Simply put, an SSTable is a logfile with key-values sorted by the key. This provides a few benefits
1. Since the keys are sorted, we can use a mergesort type of algorithm while merging & compacting. In the previous
approach, since we didn't know whether the incoming key was the latest one or not, we needed to maintain an index in memory.
Everytime we saw that key, we simply overwrote it. However, if the case where the number of unique keys > greater
than available RAM, we're stuck. SSTables allow us to look at only the first entry in all the segments to know
which key is the latest (newest segment wins). This allows us to bring down our space complexity from O(N) to O(1)!!

[^1]: The book is extremely well written, I am just smooth brained.