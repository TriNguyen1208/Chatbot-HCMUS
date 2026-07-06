"use client";
import { Navbar } from "./NavBar";
import { HomePlaceholder } from "./HomePlaceHover";

export function DashboardOverview() {
  return (
    <div className="min-h-screen bg-gray-50 w-full">
      <Navbar />
      <main className="max-w-4xl mx-auto px-4 py-8">
        <HomePlaceholder />
      </main>
    </div>
  );
}