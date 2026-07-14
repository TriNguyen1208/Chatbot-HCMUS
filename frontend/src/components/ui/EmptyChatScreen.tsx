import { MessageSquare } from "lucide-react";

export default function EmptyChatScreen() {
  return (
    // The outer container takes up all available space in the parent (e.g., the area next to the sidebar)
    <div className="w-full h-full flex flex-col items-center justify-center p-6">
      {/* Inner wrapper for spacing and alignment */}
      <div className="flex flex-col items-center justify-center gap-4 text-center">
        {/* Icon container with a subtle background circle (optional, but good for UI) */}
        <div className="flex items-center justify-center w-20 h-20 rounded-full bg-gray-100/50">
          <MessageSquare
            size={40}
            className="text-gray-400"
            strokeWidth={1.5}
          />
        </div>

        {/* Text content */}
        <div className="flex flex-col gap-1">
          <h2 className="text-xl font-semibold text-gray-800">Your Messages</h2>
          <p className="text-sm text-gray-500 max-w-sm">
            Select an existing conversation from the sidebar or start a new one
            to begin chatting.
          </p>
        </div>
      </div>
    </div>
  );
}
