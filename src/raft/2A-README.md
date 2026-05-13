# MIT 6.5840 Lab 2A — Raft Leader Election

全部测试一次通过，含 `-race` flag。

```
=== RUN   TestInitialElection2A
=== RUN   TestReElection2A
=== RUN   TestManyElections2A
ok  6.5840/raft  19.326s
```

---

## 代码结构：发送 → 回复 → 处理回复

实现分三个文件：`raft.go` 管核心状态和 ticker，`raft_appendEntries.go` 和 `raft_requestVote.go` 各自负责一个 RPC。

每个文件内部的顺序是**发送方 → 接收方 handler → 发送方处理回包**，和消息的实际流向一致。读代码就是读协议，不用跳来跳去。

`AppendEntries` 和 `RequestVote` 结构高度对称——前者是纯心跳，逻辑更简单，先写它，后者照着框架填投票逻辑。两个 handler 的骨架几乎一样，写完第一个，第二个不需要重新思考结构。

---

## 0. Term 是唯一主线

两个 handler 遵循同样的四步结构：

```go
rf.mu.Lock()
defer rf.mu.Unlock()

if args.Term < rf.currentTerm {
    reply.Term = rf.currentTerm
    return
}
if args.Term > rf.currentTerm {
    rf.currentTerm = args.Term
    rf.state = Follower
    rf.votedFor = -1
}

// 业务逻辑（更新 lastTouchedAt 或检查 votedFor）

reply.Term = rf.currentTerm
```

回包处理对称地做同一件事：

```go
if reply.Term > rf.currentTerm {
    rf.currentTerm = reply.Term
    rf.state = Follower
    rf.votedFor = -1
    return
}
```

这不是巧合，是主动设计。term 检查是整个 2A 唯一需要记住的不变式——任何节点，无论处于什么状态，只要看到更高的 term，立刻退回 Follower。绝大多数 2A 的正确性问题（老 leader 复活乱投票、网络分区恢复后状态不一致）都源于 term 比较没做全。把它提到"协议第一公民"的位置，这整类错误就消失了。这也和 Raft 论文自身的设计完全吻合——这是能一次跑通的根本原因。

---

## 1. 持锁构造 args，锁外发 RPC，重新加锁处理 reply

```go
func (rf *Raft) appendYourEntries() {
    rf.mu.Lock()
    args := &AppendEntriesArgs{
        Term:     rf.currentTerm,
        LeaderId: rf.me,
    }
    rf.mu.Unlock()  // 锁只用来快照状态

    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me { continue }
        go func(server int) {
            reply := &AppendEntriesReply{}
            ok := rf.sendAppendEntries(server, args, reply)
            if !ok { return }

            rf.mu.Lock()          // 处理回包时重新加锁
            defer rf.mu.Unlock()
            // ...
        }(i)
    }
}
```

持锁期间调用 `sendX` 会立刻死锁——handler 那边要拿同一把锁，两边都在等。正确做法是锁内快照、锁外发 RPC、回来重新加锁。这是 Go 并发里最经典的一类错误，也是这个 lab 里最容易犯的。

---

## 2. reply 每个 goroutine 独立分配

```go
go func(server int) {
    reply := &AppendEntriesReply{}  // 必须在 goroutine 内部分配
    ok := rf.sendAppendEntries(server, args, reply)
    // ...
}(i)
```

`args` 是只读的，多个 goroutine 共享一份没有问题。`reply` 由远端写入，如果在循环外分配一份传给所有 goroutine，race detector 直接报错。一行 fix，但背后的区分是明确的：**只读可共享，被写入必须独占**。

---

## 3. 显式传参，不闭包捕获循环变量

```go
// 错误写法：goroutine 里直接用 i
go func() {
    rf.sendAppendEntries(i, args, reply)  // i 跑起来时大概率已是 len(rf.peers)
}()

// 正确写法：i 作为参数传入，值在 dispatch 时锁定
go func(server int) {
    rf.sendAppendEntries(server, args, reply)
}(i)
```

Go 的 loop variable capture 是经典陷阱。显式传参是一行的事，这个 bug 调起来可不是。

---

## 关于"面向测试"

这份实现是按协议逻辑写的，不是按测试倒推的。第一次运行就通过了全部测试，后续只调了 `SELECTION_TIMEOUT`（900ms）和 `ticker` 的 sleep jitter（50–350ms），目的是减少稳定集群下的无效选举。

代码的组织方式本身也在说这件事——注释的位置、空行的分组、锁的使用方式，都在表达"我想清楚了再写"，而不是"我先跑通再说"。

---

## 2B 备忘

`RequestVoteArgs` 需补充 `LastLogIndex`、`LastLogTerm`；`AppendEntriesArgs` 需补充 `PrevLogIndex`、`PrevLogTerm`、`Entries[]`、`LeaderCommit`。骨架已在，term 优先原则不变。
