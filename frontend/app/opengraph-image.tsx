import { ImageResponse } from "next/og";

export const runtime = "edge";
export const alt = "Quantyc — mining intelligence terminal for ASX juniors";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

export default function OpengraphImage() {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          justifyContent: "center",
          padding: "80px 96px",
          background:
            "radial-gradient(900px 420px at 20% -10%, rgba(232,180,74,0.14), transparent 70%), #0e0e11",
          color: "#fafafa",
          fontFamily: "Georgia, serif",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "baseline",
            gap: 18,
          }}
        >
          <div
            style={{
              fontSize: 30,
              letterSpacing: 10,
              color: "#e8b44a",
              fontFamily: "monospace",
            }}
          >
            QUANTYC
          </div>
        </div>
        <div
          style={{
            marginTop: 28,
            fontSize: 64,
            fontStyle: "italic",
            lineHeight: 1.15,
            maxWidth: 900,
          }}
        >
          Mining intelligence for ASX juniors
        </div>
        <div
          style={{
            marginTop: 26,
            fontSize: 26,
            color: "#9a9aa3",
            fontFamily: "monospace",
          }}
        >
          Filings → capital structure → spot revaluations
        </div>
        <div
          style={{
            position: "absolute",
            left: 96,
            right: 96,
            bottom: 64,
            height: 2,
            background:
              "linear-gradient(90deg, transparent, rgba(232,180,74,0.6), transparent)",
            display: "flex",
          }}
        />
      </div>
    ),
    { ...size }
  );
}
