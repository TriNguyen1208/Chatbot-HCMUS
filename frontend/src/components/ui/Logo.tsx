import Image from "next/image";
import Link from "next/link";

const Logo = () => {
  return (
    <Link href={"/chat"} className="w-full flex flex-row gap-5 justify-start items-center">
      <Image src={"/HCMUS_Logo.png"} alt="HCMUS Logo" width={40} height={40}/>
      <p className="text-base text-[#535e6b] font-bold">HCMUS Chat</p>
    </Link>
  );
}

export default Logo;