'use client';
import Image from "next/image";

const ChatCard = () => {
  return (
    <div className="flex flex-row gap-2 justify-start items-center w-full hover:bg-hover hover:cursor-pointer pr-2 py-2 rounded-lg">
      <Image
        src="/CR_LM_Chess.jpg"
        alt="temporary avatar"
        width={40}
        height={40}
        className="rounded-full object-cover shrink-0 size-9"
      />
      <div className="flex-1 min-w-0 flex flex-col items-start justify-center">
        <div className="flex flex-row w-full justify-between items-center">
          <h3 className="font-bold text-sm">Dr. Alan Chen</h3>
          <p className="text-txt-extra text-xs uppercase">9:53 PM</p>
        </div>
        <p className="line-clamp-1 text-txt-extra text-sm">The latest dataset looks so weird</p>
      </div>
    </div>
  );
}

export default ChatCard