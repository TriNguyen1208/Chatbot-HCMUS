export const getAvatarInitials = (name: string): string => {
    if (!name) return "U";
    const parts = name.trim().split(" ");
    if (parts.length > 0) {
        const lastPart = parts[parts.length - 1];
        if (lastPart && lastPart.length > 0) {
            return lastPart[0]!.toUpperCase();
        }
    }
    return name[0] ? name[0].toUpperCase() : "U";
};

export const getAvatarColor = (name: string): string => {
    if (!name) return "#1890ff";
    const colors = [
        "#f56a00", "#7265e6", "#ffbf00", "#00a2ae",
        "#1890ff", "#eb2f96", "#52c41a", "#fa8c16",
        "#13c2c2", "#a0d911", "#2f54eb", "#73d13d"
    ];
    let hash = 0;
    for (let i = 0; i < name.length; i++) {
        hash = name.charCodeAt(i) + ((hash << 5) - hash);
    }
    return colors[Math.abs(hash) % colors.length] || "#1890ff";
};

export const generateAvatarURI = (name: string): string => {
    const initials = getAvatarInitials(name);
    const color = getAvatarColor(name);

    const svg = `
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">
  <rect width="100%" height="100%" fill="${color}" />
  <text x="50%" y="50%" dominant-baseline="central" text-anchor="middle" fill="white" font-family="Arial, sans-serif" font-size="40" font-weight="bold">${initials}</text>
</svg>
    `.trim();

    return `data:image/svg+xml;base64,${Buffer.from(svg).toString('base64')}`;
};
