/** Small local embedding function for repeatable store tests and examples. */
export function embedTexts(texts: string[]): number[][] {
  const terms = ["apple", "banana", "coffee"];
  return texts.map(text => {
    const words = text.toLowerCase().match(/[a-z]+/g) ?? [];
    return terms.map(term => words.filter(word => word === term).length);
  });
}
