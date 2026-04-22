"use client";

import { useEffect, useState } from "react";
import type { CompanySnapshot } from "@/types/snapshot";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

type SnapshotState =
  | { status: "loading" }
  | { status: "ok"; data: CompanySnapshot }
  | { status: "error"; message: string };

export function useCompanySnapshot(ticker: string) {
  const [state, setState] = useState<SnapshotState>({ status: "loading" });

  const fetchSnapshot = () => {
    setState({ status: "loading" });
    fetch(`${API_BASE}/api/company/${ticker.toUpperCase()}/snapshot`, {
      cache: "no-store",
    })
      .then((res) => {
        if (!res.ok) throw new Error(`${res.status}`);
        return res.json();
      })
      .then((data: CompanySnapshot) => setState({ status: "ok", data }))
      .catch((err) => setState({ status: "error", message: err.message }));
  };

  useEffect(() => {
    fetchSnapshot();
  }, [ticker]);

  return { ...state, retry: fetchSnapshot };
}
