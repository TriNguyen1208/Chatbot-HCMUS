import { useState } from "react";

// Custom hook quản lý thanh công cụ ở Sidebar (Phần có nút Tìm kiếm/Thêm bạn)
export const useFriendBar = () => {
  // Trạng thái Đóng/Mở của ô nhập liệu tìm kiếm (nếu có)
  const [isSearchOpen, setIsSearchOpen] = useState(false);

  return {
    isSearchOpen,
    setIsSearchOpen
  };
};
