# Raft 2A — 领导者选举

MIT 6.5840 · Go

---

## 核心设计：两条线

所有状态变化归结为两条互不干扰的线，全部收口在 `raft_helper.go`。

---

### 第一条线：页数（term）

`currentTerm` 是当前的"世界观"。你连现在是第几页都没搞清楚，后面所有逻辑都是妄谈。所以**页数的处理永远第一个发生**，任何 RPC handler 进来，第一件事先对齐页数。

页数的变化只有两种：

**`turnPage(term)`** — 翻页。遇到比自己更高的 term，这一页真的结束了。`currentTerm` 更新，角色退回 Follower，`votedFor` 清空。对应论文 Figure 2：*If RPC contains term T > currentTerm: set currentTerm = T, convert to follower*。

**`ripPage()`** — 撕页。自己是 Candidate，却收到了同 term 的 `AppendEntries`，说明这一页已经有人赢了，但 term 没有更高所以不需要翻页——直接撕掉这页幻想，退回 Follower，`votedFor` 清空，term 不变。

---

### 第二条线：计时器（touched）

`lastTouchedAt` 控制选举超时。刷新条件只有一个：**在我的世界观里，我承认对方是合法的存在**。

**`tryVotingFor(server)`** — 投票给某人（包括给自己）时刷新。我既然投了，我就认这个人。

**`touched()`** — 收到合法心跳时刷新。"合法"的判断在 `AppendEntries` handler 里：`args.Term >= oldTerm`，即对方至少和我在同一页。

其余任何情况，计时器一律不动。

---

### 为什么是这个顺序

页数是 Raft 的时间轴，计时器是 Raft 的心跳。先确认"我们在同一个时间轴上"，再谈"这个心跳是否有效"。两条线的处理顺序不能颠倒，也不能混用——这不是风格问题，是协议正确性的要求。

---

## 文件结构

| 文件 | 职责 |
|---|---|
| `raft_helper.go` | **唯一的状态写入层。** `turnPage`、`ripPage`、`tryVotingFor`、`touched` 四个函数，全部不持锁，只在持锁上下文内调用。 |
| `raft.go` | 结构体定义、`ticker` 超时循环、`leaderTicker` 心跳循环、`Make` 初始化。 |
| `raft_requestVote.go` | 投票请求的发起（`collectOpinion`）与接收（`RequestVote` handler）。 |
| `raft_appendEntries.go` | 心跳的发送（`appendYourEntries`）与接收（`AppendEntries` handler）。2B 日志复制占位于此。 |