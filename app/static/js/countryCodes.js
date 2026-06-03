/**
 * Fetch World Bank country list and build a name → ISO2 map.
 * Note: we keep the original behaviour of filtering out "non-countries" by
 * requiring a capitalCity.
 */
export async function buildNameToIso2Map() {
  const nameToIso2 = new Map();
  try {
    const wbRes = await fetch('https://api.worldbank.org/v2/country?format=json&per_page=300');
    const wbData = await wbRes.json();
    if (wbData?.[1]) {
      wbData[1].forEach((c) => {
        const iso2 = c.iso2Code;
        if (iso2 && /^[A-Z]{2}$/.test(iso2) && c.capitalCity) {
          nameToIso2.set(c.name, iso2);
        }
      });
    }
    console.log('Built name→ISO2 map with', nameToIso2.size, 'entries');
  } catch (e) {
    console.warn(e);
  }
  return nameToIso2;
}

export function createIso2Resolver(nameToIso2) {
  /**
   * @param {string} name
   * @returns {string|null}
   */
  return function getIso2ForCountryName(name) {
    if (!name) return null;

    // Exact match
    if (nameToIso2.has(name)) return nameToIso2.get(name);

    // Case-insensitive match
    const lowerName = name.toLowerCase();
    for (const [wbName, iso2] of nameToIso2.entries()) {
      if (wbName.toLowerCase() === lowerName) return iso2;
    }

    // Small alias set for common naming differences
    const aliases = {
      'United States of America': 'US',
      USA: 'US',
      'United Kingdom': 'GB',
      Russia: 'RU',
      Czechia: 'CZ',
      'South Korea': 'KR',
      China: 'CN',
      Vietnam: 'VN',
      Iran: 'IR',
      Syria: 'SY',
      Laos: 'LA'
    };
    if (aliases[name]) return aliases[name];

    return null;
  };
}
