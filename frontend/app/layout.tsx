import type { Metadata } from "next";
import { Geist, Geist_Mono, Instrument_Serif } from "next/font/google";
import "./globals.css";
import { Sidebar, MobileNav } from "@/components/sidebar";
import { CommandPalette } from "@/components/command-palette";
import { PipelineProgress } from "@/components/pipeline-progress";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

const instrumentSerif = Instrument_Serif({
  variable: "--font-instrument-serif",
  subsets: ["latin"],
  weight: "400",
  style: ["normal", "italic"],
});

export const metadata: Metadata = {
  title: "Quantyc",
  description: "Mining intelligence terminal",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} ${instrumentSerif.variable} dark h-full`}
    >
      <body className="min-h-full tabular-nums">
        <Sidebar />
        <MobileNav />
        <CommandPalette />
        <main className="lg:pl-[210px]">
          <div className="mx-auto w-full max-w-6xl px-5 py-8 lg:px-10">
            <div className="mb-4">
              <PipelineProgress />
            </div>
            {children}
          </div>
        </main>
      </body>
    </html>
  );
}
