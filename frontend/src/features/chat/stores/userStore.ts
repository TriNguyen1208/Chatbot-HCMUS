import { create } from "zustand";
import { User } from "../types";
import { userApi } from "../api/user.api";

interface UserStore {
  users: Record<string, User>;
  pendingIds: string[];
  fetchingIds: string[];
  failedIds: string[];
  fetchTimeout: ReturnType<typeof setTimeout> | null;
  
  // Actions
  addUser: (user: User) => void;
  requestUser: (id: string) => void;
  updateUserPresence: (userId: string, is_online: boolean, last_active?: string | Date) => void;
  _processQueue: () => Promise<void>;
}

export const useUserStore = create<UserStore>((set, get) => ({
  users: {},
  pendingIds: [],
  fetchingIds: [],
  failedIds: [],
  fetchTimeout: null,

  addUser: (user) => {
    set((state) => ({
      users: { ...state.users, [user.id]: user },
    }));
  },

  requestUser: (id) => {
    const state = get();
    // Ignore if already fetched, in pending queue, currently fetching, or failed permanently
    if (state.users[id] || state.pendingIds.includes(id) || state.fetchingIds.includes(id) || state.failedIds.includes(id)) {
      return;
    }

    set((s) => ({ pendingIds: [...s.pendingIds, id] }));

    // Clear existing timeout to debounce
    if (state.fetchTimeout) {
      clearTimeout(state.fetchTimeout);
    }

    // Schedule a bulk fetch in 50ms
    const timeout = setTimeout(() => {
      get()._processQueue();
    }, 50);

    set({ fetchTimeout: timeout });
  },

  updateUserPresence: (userId, is_online, last_active) => {
    set((state) => {
      const user = state.users[userId];
      if (!user) return state; // Don't update if user doesn't exist in store yet
      return {
        users: {
          ...state.users,
          [userId]: {
            ...user,
            is_online,
            ...(last_active !== undefined ? { last_active } : {})
          }
        }
      };
    });
  },

  _processQueue: async () => {
    const { pendingIds } = get();
    if (pendingIds.length === 0) return;

    // Move pendingIds to fetchingIds so new requests can queue up without overlapping
    set((state) => ({ 
      pendingIds: [], 
      fetchingIds: [...state.fetchingIds, ...pendingIds],
      fetchTimeout: null 
    }));

    try {
      const response = await userApi.getBulkUsers(pendingIds);
      const fetchedUsers = response.data
      if (Array.isArray(fetchedUsers)) {
        set((state) => {
          const newUsers = { ...state.users };
          const fetchedIds = new Set(fetchedUsers.map(u => u.id));
          
          fetchedUsers.forEach((u: User) => {
            newUsers[u.id] = u;
          });

          // Any requested ID that was not returned by the API is considered failed/missing
          const newlyFailedIds = pendingIds.filter(id => !fetchedIds.has(id));

          return { 
            users: newUsers,
            fetchingIds: state.fetchingIds.filter(id => !pendingIds.includes(id)),
            failedIds: [...state.failedIds, ...newlyFailedIds]
          };
        });
      }
    } catch (error) {
      console.error("Failed to bulk fetch users:", error);
      // On error, remove them from fetchingIds so they can be retried later
      set((state) => ({
        fetchingIds: state.fetchingIds.filter(id => !pendingIds.includes(id))
      }));
    }
  },
}));
