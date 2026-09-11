"use client";

import { useEffect, useState, useRef } from "react";
import { useSearchStore } from "@/features/chat/stores/searchStore";
import { searchApi, SearchResult } from "@/features/chat/api/search.api";

export const useSearchSidebarContent = () => {
  const { searchQuery, activeTab, setActiveTab } = useSearchStore();
  const [results, setResults] = useState<SearchResult[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const debounceTimeout = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    if (!searchQuery.trim()) {
      setResults([]);
      return;
    }

    setIsLoading(true);
    if (debounceTimeout.current) clearTimeout(debounceTimeout.current);

    debounceTimeout.current = setTimeout(async () => {
      try {
        const data = await searchApi.globalSearch(searchQuery);
        setResults(data);
      } catch (error) {
        console.error("Search failed:", error);
      } finally {
        setIsLoading(false);
      }
    }, 500);

    return () => {
      if (debounceTimeout.current) clearTimeout(debounceTimeout.current);
    };
  }, [searchQuery]);

  const filteredResults = activeTab === "all"
    ? results
    : results.filter((r) => r.search_type === activeTab);

  return {
    searchQuery,
    activeTab,
    setActiveTab,
    filteredResults,
    isLoading,
  };
};
