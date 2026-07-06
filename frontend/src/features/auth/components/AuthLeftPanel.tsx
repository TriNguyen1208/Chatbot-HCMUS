export function AuthLeftPanel() {
  return (
    <div className="relative flex-1 bg-[#0F1C2E] flex flex-col justify-between p-10 overflow-hidden">
      <div className="absolute -top-32 -left-32 w-[500px] h-[500px] rounded-full border border-white/5" />
      <div className="absolute -bottom-40 -right-40 w-[600px] h-[600px] rounded-full border border-white/5" />
      <div className="absolute bottom-0 right-0 w-80 h-80 bg-[#2563EB]/10 rounded-full blur-3xl" />

      <div className="relative z-10 flex items-center gap-3">
        <div className="w-9 h-9 rounded-xl bg-[#2563EB] flex items-center justify-center">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none">
            <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </div>
        <span className="text-white font-semibold text-lg tracking-tight">SchoolConnect</span>
      </div>

      <div className="relative z-10">
        <p className="text-[#2563EB] text-sm font-medium uppercase tracking-widest mb-4">Dành riêng cho sinh viên HCMUS</p>
        <h1 className="text-white text-4xl lg:text-5xl font-bold leading-[1.15] mb-6">
          Kết nối với<br /><span className="text-[#60A5FA]">cộng đồng</span><br />của bạn.
        </h1>
        <p className="text-white/50 text-base leading-relaxed max-w-xs">
          Nhắn tin, chia sẻ và giao lưu với bạn bè cùng trường — an toàn, riêng tư, chỉ dành cho sinh viên.
        </p>
      </div>

      <div className="relative z-10 flex flex-wrap gap-3">
        {["Tin nhắn riêng tư", "Bảng tin trường", "Nhóm học tập"].map((f) => (
          <span key={f} className="px-4 py-2 rounded-full bg-white/5 border border-white/10 text-white/60 text-sm">{f}</span>
        ))}
      </div>
    </div>
  );
}