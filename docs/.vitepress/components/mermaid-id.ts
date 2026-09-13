let nextMermaidId = 0

export function createMermaidId() {
  return `f1-mermaid-${nextMermaidId++}`
}
