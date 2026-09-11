"use client";

import { useEffect, useState } from "react";

export const useMediaViewerModal = () => {
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  return {
    mounted,
  };
};
