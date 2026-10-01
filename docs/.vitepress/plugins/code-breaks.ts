import type MarkdownIt from 'markdown-it'

// Long dotted identifiers in inline code (config keys such as
// broker.rabbitmq.consumerTimeout, destination names, metric names) have no
// break opportunity, so one of them can force a table wider than the content
// column. Add a <wbr> after each dot or slash so the browser may wrap there.
// <wbr> is not a character, so copying the identifier still copies it exactly.
const minimumLength = 20

export function codeBreaksPlugin(md: MarkdownIt) {
  const render = md.renderer.rules.code_inline!
  md.renderer.rules.code_inline = (tokens, idx, options, env, self) => {
    const html = render(tokens, idx, options, env, self)
    if (tokens[idx].content.length < minimumLength) {
      return html
    }
    const open = html.indexOf('>') + 1
    const close = html.lastIndexOf('</code>')
    const body = html.slice(open, close).replace(/([./])(?=[^./])/g, '$1<wbr>')
    return html.slice(0, open) + body + html.slice(close)
  }
}
