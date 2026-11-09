import { Engine, MemoryRepositories, key, threadKey, ticketOutcome } from '../src/core/index.ts'
import type { Policy, ReplyStatus, Trigger } from '../src/core/index.ts'

const source = 'local'
const member = 'alice'
const scope = '整理当前会话的验收清单'
const repo = new MemoryRepositories()
const jobs: Array<() => void> = []
const replies = new Map<string, { status: ReplyStatus }>()

const policy: Policy = {
  version: 'demo-v1',
  paused: false,
  evalTimeoutMs: 60_000,
  gate(trigger) {
    if (!trigger.human || trigger.actor.id !== member) return { admissible: false, reason: 'not_a_member' }
    if (!trigger.addressed) return { admissible: false, reason: 'no_mention' }
    return { admissible: true, strength: 'explicit', basis: { via: 'addressed', addressed: trigger.addressed, sender: member } }
  },
  revalidate: mandate => mandate.actor.id === member,
  admitsInferred: () => false,
}

// These ports queue work during Engine commands. Callbacks run after the command returns.
const engine = new Engine({
  repo,
  policy: () => policy,
  scope: { scope: () => ({ listened: true, observed: false }) },
  emit(event) {
    if (event.type !== 'decision.recorded') return
    jobs.push(() => {
      const decision = repo.decisions.get(event.data.decisionId)!
      const reasons = decision.reason.map(reason => reason.values?.text ?? reason.code)
      console.log(`Decision ${decision.threadKey} ${decision.kind} (${reasons.join(', ')})`)
    })
  },
  assessor: {
    request(candidate) {
      jobs.push(() => {
        const trigger = candidate.triggers[0]!
        console.log(`Eval ${trigger.channel?.address.message} ${candidate.strength}`)
        engine.candidateDelivered(candidate.evalId)
        engine.decide(trigger.id, {
          kind: 'accept', evalId: candidate.evalId, title: '验收清单', scope,
          reason: '白名单成员明确发起了演示任务', acknowledgement: '收到，我会整理这条会话里的验收项。',
        })
      })
    },
  },
  executor: {
    start(ticket, assignment) {
      jobs.push(() => {
        engine.assignmentStarted(ticket.id, assignment.id)
        console.log(`Assignment started (${ticket.mandate.actor.id}, ${ticket.mandate.policyVersion}, ${ticket.mandate.scope})`)
      })
    },
    deliver(ticket, assignment) {
      jobs.push(() => {
        engine.inputsDelivered(ticket.id, assignment.id, ticket.undelivered)
        engine.inputsRead(ticket.id, ticket.undelivered)
      })
    },
    stop() {},
  },
  outbox: {
    send(message) {
      if (replies.has(message.id)) return
      const receipt: { status: ReplyStatus } = { status: 'pending' }
      replies.set(message.id, receipt)
      jobs.push(() => {
        if (replies.get(message.id) !== receipt) return
        console.log(`Channel ${message.channel.thread} [${message.kind}] ${message.text}`)
        receipt.status = 'sent'
      })
    },
    status: id => replies.get(id)?.status,
    withdraw(id) {
      if (replies.get(id)?.status === 'pending') replies.delete(id)
    },
  },
})

function drainJobs() {
  for (let job = jobs.shift(); job; job = jobs.shift()) job()
}

function receive(message: { id: string; thread: string; sender: string; text: string; mention?: boolean }) {
  const thread = { source, id: message.thread }
  const trigger: Trigger = {
    id: key(source, message.id), source, threads: [thread], actor: { source, id: message.sender }, human: true,
    ...(message.mention ? { addressed: 'mention' } : {}), at: new Date().toISOString(), text: message.text,
    channel: { source, thread: threadKey(thread), address: { message: message.id } }, meta: {},
  }
  const intake = engine.receive(trigger)
  console.log(`Trigger ${message.id} ${intake}`)
  if (intake !== 'stored') return
  engine.evaluateDue()
  drainJobs()
  const disposition = repo.triggers.disposition(trigger.id)
  if (disposition?.fanout.length) console.log(`Input ${message.id} → ${disposition.fanout.length} subscribed Ticket`)
}

const request = { id: 'm2', thread: 'release', sender: member, text: '验证本地安装', mention: true }
receive({ id: 'm1', thread: 'other', sender: 'guest', text: '替我整理验收项', mention: true })
receive(request)
receive(request)
receive({ id: 'm3', thread: request.thread, sender: member, text: '检查结果是否回到原会话' })

// Simulated worker. Only the initiating actor's messages contribute items to this task.
const [ticket] = repo.tickets.open()
if (!ticket) throw new Error('Expected an open checklist Ticket')
const inputIds = ticket.inputs.flatMap(input => input.kind === 'trigger' && input.readAt ? [input.triggerId] : [])
const items = [ticket.origin.triggerId, ...inputIds]
  .map(id => repo.triggers.get(id)!)
  .filter(trigger => trigger.actor.id === ticket.mandate.actor.id)
  .map(trigger => `- [ ] ${trigger.text}`)
const summary = `验收清单\n${items.join('\n')}`
const completed = engine.report(ticket.id, { state: 'ready', summary })
engine.reply({ purpose: 'result', ticketId: completed.id, text: summary })
drainJobs()
console.log(`Ticket ${ticketOutcome(completed)} (total ${repo.tickets.list().length})`)
