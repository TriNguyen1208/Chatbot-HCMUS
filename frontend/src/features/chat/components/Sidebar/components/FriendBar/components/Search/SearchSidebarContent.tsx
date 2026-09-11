"use client";

import SearchItem from "./SearchItem";
import { useSearchSidebarContent } from "./useSearchSidebarContent";

const SearchSidebarContent = () => {
  const { searchQuery, activeTab, setActiveTab, filteredResults, isLoading } = useSearchSidebarContent();

  return (
    <div className="flex-1 flex flex-col min-h-0 w-full transition-all duration-300">
      {/* Tabs */}
      <div className="flex items-center justify-around px-2 py-2 border-b border-b-border-primary/50 shrink-0">
        <TabButton label="All" isActive={activeTab === 'all'} onClick={() => setActiveTab('all')} />
        <TabButton label="User" isActive={activeTab === 'user'} onClick={() => setActiveTab('user')} />
        <TabButton label="Conversation" isActive={activeTab === 'conversation'} onClick={() => setActiveTab('conversation')} />
        <TabButton label="Message" isActive={activeTab === 'message'} onClick={() => setActiveTab('message')} />
      </div>

      {/* Results */}
      <div className="flex-1 overflow-y-auto overflow-x-hidden scrollbar-thumb-input-surface scrollbar-track-white/40 scroll-smooth scrollbar-thin p-2 space-y-1">
        {isLoading && <div className="text-center text-sm text-txt-extra py-4">Searching...</div>}
        {!isLoading && searchQuery && filteredResults.length === 0 && (
          <div className="text-center text-sm text-txt-extra py-4">No results found.</div>
        )}
        {!isLoading && filteredResults.map((item) => (
          <SearchItem key={`${item.search_type}_${item.id}`} item={item} />
        ))}
      </div>
    </div>
  );
};

const TabButton = ({ label, isActive, onClick }: { label: string; isActive: boolean; onClick: () => void }) => {
  return (
    <button 
      onClick={onClick}
      className={`cursor-pointer text-xs font-medium px-2 py-1.5 rounded-md transition-colors ${
        isActive ? 'bg-brand-primary text-white' : 'text-txt-extra hover:bg-glass-panel hover:text-txt-primary'
      }`}
    >
      {label}
    </button>
  );
};

export default SearchSidebarContent;
