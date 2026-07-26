import Sidebar from "@/components/chat/Sidebar";

const layout = ({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) => {
  return <main className="grid grid-cols-9 w-screen h-screen">
    <div className="bg-white col-span-2 w-full h-full">
      <Sidebar/>
    </div>
    <div className="bg-white col-span-7 w-full h-full">{children}</div>
  </main>;
};

export default layout 