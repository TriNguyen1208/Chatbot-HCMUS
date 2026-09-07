import { create } from 'zustand';

export type SearchTab = 'all' | 'user' | 'conversation' | 'message';

export interface SearchState {
    isSearchMode: boolean;
    searchQuery: string;
    activeTab: SearchTab;
    targetMessageId: string | null;
    
    setSearchMode: (isSearchMode: boolean) => void;
    setSearchQuery: (query: string) => void;
    setActiveTab: (tab: SearchTab) => void;
    setTargetMessageId: (messageId: string | null) => void;
    resetSearch: () => void;
}

export const useSearchStore = create<SearchState>()((set) => ({
    isSearchMode: false,
    searchQuery: '',
    activeTab: 'all',
    targetMessageId: null,

    setSearchMode: (isSearchMode) => set({ isSearchMode }),
    setSearchQuery: (searchQuery) => set({ searchQuery }),
    setActiveTab: (activeTab) => set({ activeTab }),
    setTargetMessageId: (targetMessageId) => set({ targetMessageId }),
    resetSearch: () => set({ 
        isSearchMode: false, 
        searchQuery: '', 
        activeTab: 'all', 
        targetMessageId: null 
    })
}));
