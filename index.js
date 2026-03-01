// index.js
import express from "express";
import YahooFinance from "yahoo-finance2";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

// ---------------- Supabase ----------------
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ---------------- Symbol Conversion ----------------

// Convert DB symbol to Yahoo format
function convertToYahoo(symbol) {
  if (!symbol) return null;
  symbol = symbol.trim();
  if (symbol.startsWith("NSE:")) return symbol.replace("NSE:", "").replace(/\./g, "") + ".NS";
  if (symbol.startsWith("BOM:")) return symbol.replace("BOM:", "").replace(/\./g, "") + ".BO";
  return symbol.replace(/\./g, ""); // fallback: remove dots
}

// Convert Yahoo symbol back to DB format
function convertToDB(yahooSymbol) {
  if (!yahooSymbol) return null;
  if (yahooSymbol.endsWith(".NS")) return "NSE:" + yahooSymbol.replace(".NS", "");
  if (yahooSymbol.endsWith(".BO")) return "BOM:" + yahooSymbol.replace(".BO", "");
  return yahooSymbol;
}

// Generate possible Yahoo symbol variants for a stock
function generateYahooVariants(dbSymbol) {
  const variants = [];
  const baseYahoo = convertToYahoo(dbSymbol);
  if (!baseYahoo) return variants;

  variants.push(baseYahoo);

  // SME alternative
  if (baseYahoo.endsWith(".NS")) variants.push(baseYahoo.replace(".NS", "-SM.NS"));
  if (baseYahoo.endsWith(".BO")) variants.push(baseYahoo.replace(".BO", "-SM.BO"));

  // Try switching NSE/BSE if first fails
  if (baseYahoo.endsWith(".NS")) variants.push(baseYahoo.replace(".NS", ".BO"));
  if (baseYahoo.endsWith(".BO")) variants.push(baseYahoo.replace(".BO", ".NS"));

  return variants;
}

// ---------------- Main Route ----------------
app.get("/fill-missing-cmp", async (req, res) => {
  try {
    console.log("🔍 Checking missing CMP/LCP...");

    // Fetch missing rows (cmp or lcp null or 0)
    const { data: stocks, error } = await supabase
      .from("stock_master")
      .select("symbol, stock_name")
      .or("cmp.is.null,cmp.eq.0,lcp.is.null,lcp.eq.0")
      .limit(1000);

    if (error) throw error;
    if (!stocks || stocks.length === 0) return res.json({ message: "No missing stocks found." });

    console.log(`📊 Found ${stocks.length} stocks missing CMP/LCP`);

    const batchSize = 50; // safe batch size
    let totalUpdated = 0;
    let zeroValues = 0;
    const failedSymbols = [];

    // Process in batches
    for (let i = 0; i < stocks.length; i += batchSize) {
      const batch = stocks.slice(i, i + batchSize);
      console.log(`⚡ Processing batch ${i / batchSize + 1}`);

      await Promise.all(batch.map(async (stock) => {
        const dbSymbol = stock.symbol;
        const stockName = stock.stock_name || null;
        let yfData = null;

        const variants = generateYahooVariants(dbSymbol);

        // Try each variant until valid CMP is found
        for (const sym of variants) {
          try {
            yfData = await YahooFinance.default(sym);
            if (yfData && yfData.regularMarketPrice != null) break;
          } catch {}
        }

        if (!yfData || yfData.regularMarketPrice == null) {
          failedSymbols.push({ symbol: dbSymbol, name: stockName });
          return;
        }

        const cmpVal = yfData.regularMarketPrice ?? 0;
        const lcpVal = yfData.regularMarketPreviousClose ?? 0;

        if (cmpVal === 0 || lcpVal === 0) zeroValues++;

        await supabase
          .from("stock_master")
          .update({ cmp: cmpVal, lcp: lcpVal })
          .eq("symbol", dbSymbol);

        totalUpdated++;
      }));
    }

    // Summary
    console.log(`✅ Total rows updated with valid values: ${totalUpdated - zeroValues}`);
    console.log(`⚠ Total rows updated with 0 or null: ${zeroValues}`);
    if (failedSymbols.length > 0) {
      console.log(`❌ Symbols not updated by Yahoo Finance:`);
      failedSymbols.forEach(f => console.log(`- ${f.symbol}${f.name ? ` (${f.name})` : ""}`));
    }

    res.json({
      message: "Yahoo Finance update completed",
      totalUpdated,
      zeroValues,
      failedSymbols
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ---------------- Start Server ----------------
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});