import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "../assets/styles/globals.css";
import { Providers } from "./providers";

const inter = Inter({ subsets: ["latin", "vietnamese"], variable: '--font-inter' });

export const metadata: Metadata = {
  title: "HCMUS Chatbot - Nền tảng nhắn tin & hỗ trợ sinh viên",
  description: "Ứng dụng nhắn tin và trợ lý thông minh cho sinh viên Đại học Khoa học Tự nhiên TP.HCM",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="vi" suppressHydrationWarning>
      <body className={`${inter.className} min-h-screen bg-background text-foreground antialiased transition-colors duration-300`}>
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
