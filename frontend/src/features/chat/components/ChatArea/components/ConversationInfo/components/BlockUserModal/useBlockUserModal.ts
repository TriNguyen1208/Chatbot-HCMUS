"use client";

import { useEffect, useState } from "react";

export const useBlockUserModal = () => {
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  return {
    mounted,
  };
};
