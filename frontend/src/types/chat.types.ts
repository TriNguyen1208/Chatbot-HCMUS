export type MessageType = "text" | "file" | "link" | "image" | "video" | "ai" | "system";
export type MessageStatus = "sent" | "received" | "recalled" | "removed";

export interface Reaction {
  user_id?: string;
  emoji: string;
}

export interface Message {
  id?: string;
  sender_id?: string;
  conversation_id?: string;
  content?: string;
  type: MessageType;
  status?: MessageStatus;
  image?: {
    url: string;
    file_key?: string;
  };
  video?: {
    url?: string;
    file_key: string;
    thumbnail_url?: string;
  };
  tag_ids?: string[];
  reactions?: Reaction[];
  created_at?: Date | string;
  updated_at?: Date | string;
  is_edited?: boolean;
  edit_history?: { content: string; updated_at: Date | string }[];
}

export type ConversationType = "group" | "utu" | "self";

export interface Watermark {
  user_id: string;
  last_delivered_msg_id?: string | null;
  last_read_msg_id?: string | null;
}

export interface BlockInfo {
  block_by: string;
  block_at: Date | string;
}

export interface Conversation {
  id?: string;
  member_ids?: string[];
  admin_ids?: string[];
  last_message?: Message | null;
  created_at?: Date | string;
  name?: string;
  avatar_url?: string;
  type: ConversationType;
  primary_icon?: string;
  is_active?: boolean;
  receiver_id?: string;
  watermarks?: Watermark[];
  block?: BlockInfo | null;
}
