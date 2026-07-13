import Sidebar from "@/components/chat/Sidebar";

const layout = ({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) => {
  return <main className="grid grid-cols-5 w-screen h-screen">
    <div className="bg-white col-span-1 w-full h-full">
      <Sidebar/>
    </div>
    <div className="bg-white col-span-4 w-full h-full">{children}</div>
  </main>;
};

export default layout 