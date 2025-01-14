import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const SYMBOLS = [
  "AALR3",
  "ABCB4",
  "ABEV3",
  "AERI3",
  "AESB3",
  "AGRO3",
  "ALPA4",
  "ALOS3",
  "ALUP11",
  "AMBP3",
  "ANIM3",
  "ARML3",
  "ARZZ3",
  "ASAI3",
  "AURE3",
  "AZUL4",
  "B3SA3",
  "BBAS3",
  "BBDC3",
  "BBDC4",
  "BBSE3",
  "BEEF3",
  "BHIA3",
  "BLAU3",
  "BMOB3",
  "BPAC11",
  "BPAN4",
  "BRAP4",
  "BRFS3",
  "BRKM5",
  "BRPR3",
  "BRSR6",
  "CAML3",
  "CASH3",
  "CBAV3",
  "CCRO3",
  "CEAB3",
  "CIEL3",
  "CLSA3",
  "CMIG3",
  "CMIG4",
  "CMIN3",
  "COGN3",
  "CPFE3",
  "CPLE6",
  "CRFB3",
  "CSAN3",
  "CSMG3",
  "CSNA3",
  "CURY3",
  "CVCB3",
  "CXSE3",
  "CYRE3",
  "DASA3",
  "DIRR3",
  "DXCO3",
  "ECOR3",
  "EGIE3",
  "ELET3",
  "ELET6",
  "EMBR3",
  "ENAT3",
  "ENEV3",
  "ENGI11",
  "EQTL3",
  "ESPA3",
  "EVEN3",
  "EZTC3",
  "FESA4",
  "FLRY3",
  "FRAS3",
  "GFSA3",
  "GGBR4",
  "GGPS3",
  "GMAT3",
  "GOAU4",
  "GOLL4",
  "GRND3",
  "GUAR3",
  "HAPV3",
  "HBSA3",
  "HYPE3",
  "IFCM3",
  "IGTI11",
  "INTB3",
  "IRBR3",
  "ITSA4",
  "ITUB3",
  "ITUB4",
  "JALL3",
  "JBSS3",
  "JHSF3",
  "KEPL3",
  "KLBN11",
  "LAVV3",
  "LEVE3",
  "LJQQ3",
  "LOGG3",
  "LOGN3",
  "LREN3",
  "LUPA3",
  "LWSA3",
  "MATD3",
  "MBLY3",
  "MDIA3",  
  "MEGA3",
  "MGLU3",
  "MILS3",
  "MLAS3",
  "MOVI3",
  "MRFG3",
  "MRVE3",
  "MULT3",
  "MYPK3",
  "NEOE3",
  "NTCO3",
  "ODPV3",
  "ONCO3",
  "ORVR3",
  "PCAR3",
  "PETR3",
  "PETR4",
  "PETZ3",
  "PGMN3",
  "PLPL3",
  "PNVL3",
  "POMO4",
  "POSI3",
  "PRIO3",
  "PSSA3",
  "PTBL3",
  "QUAL3",
  "RADL3",
  "RAIL3",
  "RAIZ4",
  "RANI3",
  "RAPT4",
  "RDOR3",
  "RECV3",
  "RENT3",
  "ROMI3",
  "RRRP3",
  "SANB11",
  "SAPR11",
  "SBFG3",
  "SBSP3",
  "SEER3",
  "SEQL3",
  "SIMH3",
  "SLCE3",
  "SMFT3",
  "SMTO3",
  "SOMA3",
  "SQIA3",
  "STBP3",
  "SUZB3",
  "TAEE11",
  "TASA4",
  "TEND3",
  "TGMA3",
  "TIMS3",
  "TOTS3",
  "TRIS3",
  "TRPL4",
  "TTEN3",
  "TUPY3",
  "UGPA3",
  "UNIP6",
  "USIM5",
  "VALE3",
  "VAMO3",
  " VBBR3",
  "VIVA3",
  "VIVT3",
  "VLID3",
  "VULC3",
  "WEGE3",
  "WIZC3",
  "YDUQ3",
  "ZAMP3",
];

async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getLastUpdateDate(symbol: string) {
  const { data: stock } = await supabase
    .from("stocks")
    .select("id")
    .eq("symbol", symbol)
    .single();

  if (!stock) return null;

  const { data } = await supabase
    .from("historical_data")
    .select("date")
    .eq("stock_id", stock.id)
    .order("date", { ascending: false })
    .limit(1)
    .single();

  return data?.date;
}

async function fetchStockInfo(symbol: string, startDate?: Date) {
  // Adiciona delay para evitar sobrecarga
  await sleep(500);

  // Agora busca os dados históricos
  const params = new URLSearchParams({
    interval: '1d',
    events: 'history'
  });

  if (startDate) {
    // Garante que a data final seja hoje às 23:59:59
    const endDate = new Date();
    endDate.setHours(23, 59, 59, 999);
    
    // Garante que a data inicial seja no início do dia
    startDate.setHours(0, 0, 0, 0);
    
    // Verifica se a data inicial não é futura
    if (startDate > endDate) {
      console.log(`Data inicial ${startDate} é futura para ${symbol}, usando data atual`);
      startDate = new Date();
      startDate.setHours(0, 0, 0, 0);
    }

    params.append('period1', Math.floor(startDate.getTime() / 1000).toString());
    params.append('period2', Math.floor(endDate.getTime() / 1000).toString());
  } else {
    params.append('range', 'max');
  }

  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}.SA?${params}`;
  
  const response = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)'
    }
  });
  const data = await response.json();
  
  if (data.chart.error) {
    throw new Error(data.chart.error.description);
  }

  return {
    name: symbol,
    history: data.chart.result[0]
  };
}

async function processSymbol(symbol: string) {
  try {
    console.log(`Iniciando coleta de ${symbol}`);

    // Verifica última data
    const lastDate = await getLastUpdateDate(symbol);
    const startDate = lastDate ? new Date(lastDate) : null;
    if (startDate) {
      startDate.setDate(startDate.getDate() + 1);
    }

    // Coleta dados do Yahoo Finance
    const { history } = await fetchStockInfo(symbol, startDate);

    // Insere/atualiza stock (apenas com o símbolo)
    const { data: stock, error: stockError } = await supabase
      .from("stocks")
      .upsert({ symbol }, { onConflict: 'symbol' })
      .select()
      .single();

    if (stockError || !stock) {
      throw new Error(`Falha ao inserir/atualizar stock: ${stockError?.message || 'Sem dados retornados'}`);
    }

    // Prepara dados históricos
    const timestamps = history.timestamp || [];
    const quotes = history.indicators.quote[0];
    const historicalData = timestamps.map((timestamp: number, index: number) => ({
      stock_id: stock.id,
      date: new Date(timestamp * 1000).toISOString().split('T')[0],
      open: quotes.open[index],
      high: quotes.high[index],
      low: quotes.low[index],
      close: quotes.close[index],
      volume: quotes.volume[index]
    }));

    // Insere dados históricos
    if (historicalData.length > 0) {
      const { error: historyError } = await supabase
        .from("historical_data")
        .upsert(historicalData, { onConflict: 'stock_id,date' });

      if (historyError) {
        throw historyError;
      }
    }

    console.log(`${symbol} processado com sucesso! ${historicalData.length} registros inseridos.`);
  } catch (error) {
    console.error(`Erro ao processar ${symbol}:`, error);
  }
}

serve(async (req) => {
  try {
    const startTime = new Date();
    console.log(`Iniciando coleta de dados - ${startTime}`);

    // Processa símbolos em paralelo, mas com limite de 3 concurrent
    const chunks = [];
    const chunkSize = 3;
    for (let i = 0; i < SYMBOLS.length; i += chunkSize) {
      chunks.push(SYMBOLS.slice(i, i + chunkSize));
    }

    for (const chunk of chunks) {
      await Promise.all(chunk.map((symbol) => processSymbol(symbol)));
    }

    const endTime = new Date();
    const duration = (endTime.getTime() - startTime.getTime()) / 1000;
    console.log(`Coleta finalizada - Duração: ${duration}s`);

    return new Response(
      JSON.stringify({
        success: true,
        message: `Dados coletados com sucesso em ${duration}s`,
      }),
      {
        headers: { "Content-Type": "application/json" },
      }
    );
  } catch (error) {
    return new Response(
      JSON.stringify({
        success: false,
        error: error.message,
      }),
      {
        status: 500,
        headers: { "Content-Type": "application/json" },
      }
    );
  }
});
