import Image from "next/image";
import Link from "next/link";

const Logo = () => {
  return (
    <Link href={"/chat"} className="w-full flex flex-row gap-3 justify-start items-center">
      <Image src={"/HCMUS_Logo.png"} alt="HCMUS Logo" width={40} height={40}/>
      <p className="text-xl text-brand-primary font-bold">HCMUS CHAT</p>
    </Link>
  );
}

export default Logo;