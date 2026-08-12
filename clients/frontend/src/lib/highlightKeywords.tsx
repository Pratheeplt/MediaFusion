import type React from 'react'

export function highlightKeywords(text: string, keywords: string[]): React.ReactNode {
  if (!keywords.length) return text
  const sorted = [...keywords].sort((a, b) => b.length - a.length)
  const pattern = new RegExp(`(${sorted.map((k) => k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')})`, 'gi')
  const parts = text.split(pattern)
  return parts.map((part, i) =>
    pattern.test(part) ? (
      <mark key={i} className="bg-orange-400/30 text-orange-300 rounded px-0.5 font-semibold not-italic">
        {part}
      </mark>
    ) : (
      part
    ),
  )
}

export function keywordBlockTitle(matchedKeywords?: string[]): string {
  if (matchedKeywords?.length) {
    return `Blocked by keyword filter: ${matchedKeywords.join(', ')}`
  }
  return 'Blocked by a keyword filter and not visible to regular users.'
}
