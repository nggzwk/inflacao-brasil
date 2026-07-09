import type { BasketItemData, BasketSummaryProps } from "./basketTypes";

export const VEGGIE_BASKET_LABEL = "Feirao";
export const VEGGIE_BASKET_SUBTITLE = "Itens monitorados";
export const VEGGIE_BASKET_TOOLTIP = "Inflacao da cesta de hortifruti mensal mes sobre mes";

export const VEGGIE_ICONS: Record<number, string> = {
  50008: "🍅", // Tomate Comum
  50009: "🍅", // Tomate Rasteiro (fallback)
  50025: "🍌", // Banana Prata
  50024: "🍌", // Banana Caturra (fallback)
  50005: "🥔", // Batata Inglesa
  50002: "🧅", // Cebola
  50079: "🥬", // Alface Americana
  50080: "🥬", // Alface (fallback)
  50007: "🥕", // Cenoura
  50021: "🍊", // Laranja Pera
  50017: "🎃", // Abobora
  50028: "🍎", // Maca Fuji
  50029: "🍎", // Maca Gala (fallback)
  50004: "🍠", // Batata Doce
};

export const VEGGIE_SUBTITLES: Record<number, string> = {
  50008: "COMUM",
  50009: "RASTEIRO",
  50025: "PRATA",
  50024: "CATURRA",
  50005: "INGLESA",
  50002: "NACIONAL",
  50079: "AMERICANA",
  50080: "CRESPA",
  50007: "NACIONAL",
  50021: "PERA",
  50017: "NACIONAL",
  50028: "FUJI",
  50029: "GALA",
  50004: "NACIONAL",
};

export function getVeggieItemIcon(subcat: number): string {
  return VEGGIE_ICONS[subcat] ?? "🥦";
}

export function getVeggieItemSubtitle(subcat: number): string {
  return VEGGIE_SUBTITLES[subcat] ?? "toque para voltar";
}

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? process.env.API_BASE_URL ?? "http://localhost:8000";

export type VeggieBasketApiResponse = {
  items: Array<{
    produto_categoria: number;
    produto_subcategoria: number;
    item_name: string;
    qtd_embalagem: string;
    month_ref: string;
    month_price: number | string | null;
    previous_price: number | string | null;
    mom_pct: number | null;
    ipca_monthly_pct: number | null;
  }>;
};

export type VeggieInflationApiResponse = Array<{
  month_ref: string;
  actual_month_value_brl: number;
  previous_month_value_brl: number | null;
  basket_difference_brl: number | null;
  inflation_pct: number | null;
  ipca_monthly_pct: number | null;
  annual_ipca_pct: number | null;
}>;

export async function getVeggieBasketItems(
  month_ref?: string
): Promise<VeggieBasketApiResponse | null> {
  try {
    const url = month_ref
      ? `${API_BASE_URL}/api/vegetable-basket/items/price?month_ref=${month_ref}`
      : `${API_BASE_URL}/api/vegetable-basket/items/price`;

    const res = await fetch(url, { next: { revalidate: 86400 } });
    if (!res.ok) return null;
    return (await res.json()) as VeggieBasketApiResponse;
  } catch {
    return null;
  }
}

export async function getVeggieBasketSummaryProps(): Promise<BasketSummaryProps> {
  const empty: BasketSummaryProps = {
    items: [],
    totalValue: 0,
    totalInflationPct: 0,
    monthlyIpca: null,
    annualIpca: null,
    ipcaMonthRef: null,
  };

  try {
    const [itemsResponse, inflationResponse] = await Promise.all([
      fetch(`${API_BASE_URL}/api/vegetable-basket/items/price`, {
        next: { revalidate: 86400 },
      }),
      fetch(`${API_BASE_URL}/api/vegetable-basket/inflation/month`, {
        next: { revalidate: 86400 },
      }),
    ]);

    if (!itemsResponse.ok || !inflationResponse.ok) {
      return empty;
    }

    const itemsData = (await itemsResponse.json()) as VeggieBasketApiResponse;
    const inflationData = (await inflationResponse.json()) as VeggieInflationApiResponse;

    const latestMonthRef = itemsData.items[0]?.month_ref ?? null;
    const latestItems = latestMonthRef
      ? itemsData.items.filter((item) => item.month_ref === latestMonthRef)
      : itemsData.items;

    const latestInflation = inflationData[0];
    const latestIpcaEntry = inflationData.find(
      (e) => e.ipca_monthly_pct !== null && e.annual_ipca_pct !== null
    ) ?? null;

    return {
      items: latestItems.map((item) => ({
        ...item,
        month_price: item.month_price === null ? "0" : String(item.month_price),
        previous_price:
          item.previous_price === null ? null : String(item.previous_price),
      })),
      totalValue: latestInflation?.basket_difference_brl ?? 0,
      totalInflationPct: latestInflation?.inflation_pct ?? 0,
      monthlyIpca: latestIpcaEntry?.ipca_monthly_pct ?? null,
      annualIpca: latestIpcaEntry?.annual_ipca_pct ?? null,
      ipcaMonthRef: latestIpcaEntry?.month_ref ?? null,
    };
  } catch {
    return empty;
  }
}

export async function getVeggieBasketDataForMonth(
  month_ref: string
): Promise<BasketSummaryProps | null> {
  try {
    const [itemsRes, inflationRes] = await Promise.all([
      fetch(`${API_BASE_URL}/api/vegetable-basket/items/price?month_ref=${month_ref}`, {
        next: { revalidate: 86400 },
      }),
      fetch(
        `${API_BASE_URL}/api/vegetable-basket/inflation/month?month_ref=${month_ref}`,
        { next: { revalidate: 86400 } }
      ),
    ]);

    if (!itemsRes.ok) return null;

    const itemsData = (await itemsRes.json()) as VeggieBasketApiResponse;
    const inflationData = inflationRes.ok
      ? ((await inflationRes.json()) as VeggieInflationApiResponse)
      : [];

    const latestInflation = inflationData[0];
    const latestIpcaEntry = inflationData.find(
      (e) => e.ipca_monthly_pct !== null && e.annual_ipca_pct !== null
    ) ?? null;

    return {
      items: itemsData.items.map((item) => ({
        ...item,
        month_price: item.month_price === null ? "0" : String(item.month_price),
        previous_price:
          item.previous_price === null ? null : String(item.previous_price),
      })),
      totalValue: latestInflation?.basket_difference_brl ?? 0,
      totalInflationPct: latestInflation?.inflation_pct ?? 0,
      monthlyIpca: latestIpcaEntry?.ipca_monthly_pct ?? null,
      annualIpca: latestIpcaEntry?.annual_ipca_pct ?? null,
      ipcaMonthRef: latestIpcaEntry?.month_ref ?? null,
    };
  } catch {
    return null;
  }
}

export async function getVeggieAvailableMonths(year: number): Promise<string[]> {
  const now = new Date();
  const lastMonth = year < now.getFullYear() ? 12 : now.getMonth() + 1;

  const results = await Promise.all(
    Array.from({ length: lastMonth }, (_, i) => {
      const month_ref = `${year}-${String(i + 1).padStart(2, "0")}`;
      return fetch(
        `${API_BASE_URL}/api/vegetable-basket/items/price?month_ref=${month_ref}`,
        { next: { revalidate: 86400 } }
      )
        .then(async (res) => {
          if (!res.ok) return null;
          const { items } = (await res.json()) as VeggieBasketApiResponse;
          const ref = items[0]?.month_ref;
          return ref?.startsWith(`${year}-`) ? ref.slice(0, 7) : null;
        })
        .catch(() => null);
    })
  );

  return [...new Set(results.filter(Boolean) as string[])].sort();
}
