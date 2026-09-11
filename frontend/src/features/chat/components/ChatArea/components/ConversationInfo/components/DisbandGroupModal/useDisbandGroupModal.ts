"use client";

import { useEffect, useState } from "react";

export const useDisbandGroupModal = () => {
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  return {
    mounted,
  };
};
