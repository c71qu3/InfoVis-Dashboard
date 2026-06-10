/**
 * build topojson numeric id → ISO3.
 * reads /static/data/iso_numeric.json, which maps ISO3 -> numeric
 */
export async function buildNumericToIso3Map(url = '/static/data/iso_numeric.json') {
  const numericToIso3 = new Map();
  try {
    const isoNum = await fetch(url).then((r) => r.json());
    for (const [iso3, num] of Object.entries(isoNum)) {
      numericToIso3.set(String(num), iso3);
    }
  } catch (e) {
    console.warn(e);
  }
  return numericToIso3;
}
