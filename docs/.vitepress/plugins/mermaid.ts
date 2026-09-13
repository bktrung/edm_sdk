import type MarkdownIt from 'markdown-it'

export function mermaidPlugin(md: MarkdownIt) {
  const fence = md.renderer.rules.fence

  md.renderer.rules.fence = (...args) => {
    const [tokens, index] = args
    const token = tokens[index]

    if (token.info.trim() !== 'mermaid') {
      return fence?.(...args) ?? ''
    }

    const code = md.utils.escapeHtml(encodeURIComponent(token.content.trim()))
    return `<F1Mermaid code="${code}"></F1Mermaid>\n`
  }
}
