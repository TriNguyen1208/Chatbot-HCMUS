module.exports = {
  apps: [
    // -------------------------------------------------------------
    // 1. CHAT SERVER (HTTP & Socket.IO): 2 Instances in Cluster Mode
    // -------------------------------------------------------------
    {
      name: "chat-server",
      script: "./dist/index.js",
      instances: 2,
      exec_mode: "cluster",
      node_args: "--conditions=production",
      env: {
        NODE_ENV: "production",
        PORT: 3001
      },
      max_memory_restart: "500M",
      error_file: "./logs/server-error.log",
      out_file: "./logs/server-out.log",
      time: true,
      merge_logs: true
    },

    // -------------------------------------------------------------
    // 2. CHAT WORKER: 1 Instance in Fork Mode (BullMQ & Cron Jobs)
    // -------------------------------------------------------------
    {
      name: "chat-worker",
      script: "./dist/worker.js",
      instances: 1,
      exec_mode: "fork",
      node_args: "--conditions=production",
      env: {
        NODE_ENV: "production"
      },
      max_memory_restart: "800M",
      error_file: "./logs/worker-error.log",
      out_file: "./logs/worker-out.log",
      time: true
    }
  ]
};
