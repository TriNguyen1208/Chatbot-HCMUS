export interface FacultyProgramInfo {
    faculty: string;
    program: string;
}

export const FACULTY_PROGRAM_MAPPING: Record<string, FacultyProgramInfo> = {
    // 1xx - Khoa CNTT
    "120": { faculty: "Khoa CNTT", program: "Nhóm ngành Máy tính & CNTT" },
    "125": { faculty: "Khoa CNTT", program: "CT Tiên tiến (APCS)" },
    "126": { faculty: "Khoa CNTT", program: "CT Việt - Pháp" },
    "127": { faculty: "Khoa CNTT", program: "CT Chất lượng cao" },
    "810": { faculty: "Khoa CNTT", program: "CT Đào tạo từ xa" },
    "850": { faculty: "Khoa CNTT", program: "CT Hoàn chỉnh ĐH" },
    "880": { faculty: "Khoa CNTT", program: "CT Văn bằng 2" },
    // Cao đẳng CNTT (60 - 65)
    "60": { faculty: "Khoa CNTT", program: "Hệ Cao đẳng" },
    "61": { faculty: "Khoa CNTT", program: "Hệ Cao đẳng" },
    "62": { faculty: "Khoa CNTT", program: "Hệ Cao đẳng" },
    "63": { faculty: "Khoa CNTT", program: "Hệ Cao đẳng" },
    "64": { faculty: "Khoa CNTT", program: "Hệ Cao đẳng" },
    "65": { faculty: "Khoa CNTT", program: "Hệ Cao đẳng" },

    // 1xx & 2xx - Khoa Toán - Tin
    "110": { faculty: "Khoa Toán - Tin", program: "Nhóm ngành Toán - Tin" },
    "280": { faculty: "Khoa Toán - Tin", program: "Ngành Khoa học Dữ liệu" },

    // 1xx & 2xx - Khoa Vật lý - VLKT
    "130": { faculty: "Khoa Vật lý - VLKT", program: "Ngành Vật lý học" },
    "230": { faculty: "Khoa Vật lý - VLKT", program: "Ngành Kỹ thuật Hạt nhân" },
    "260": { faculty: "Khoa Vật lý - VLKT", program: "Ngành Vật lý Y khoa" },

    // 1xx & 2xx - Khoa Hóa học
    "140": { faculty: "Khoa Hóa học", program: "Ngành Hóa học" },
    "146": { faculty: "Khoa Hóa học", program: "CT Việt - Pháp" },
    "147": { faculty: "Khoa Hóa học", program: "CT Chất lượng cao" },
    "247": { faculty: "Khoa Hóa học", program: "Ngành CNKT Hóa học (CLC)" },
    "900": { faculty: "Khoa Hóa học", program: "Ngành CNKT Hóa học (CLC)" },

    // 1xx - Khoa Sinh học & CNSH
    "150": { faculty: "Khoa Sinh - CNSH", program: "Ngành Sinh học" },
    "157": { faculty: "Khoa Sinh - CNSH", program: "Ngành Sinh học (CLC)" },
    "180": { faculty: "Khoa Sinh - CNSH", program: "Ngành Công nghệ Sinh học" },
    "187": { faculty: "Khoa Sinh - CNSH", program: "Ngành CNSH (CLC)" },

    // 2xx - Khoa Điện tử Viễn thông
    "200": { faculty: "Khoa ĐTVT", program: "Ngành Kỹ thuật ĐTVT" },
    "207": { faculty: "Khoa ĐTVT", program: "Ngành Kỹ thuật ĐTVT (CLC)" },

    // 1xx & 2xx - Khoa Môi trường
    "170": { faculty: "Khoa Môi trường", program: "Ngành Khoa học Môi trường" },
    "177": { faculty: "Khoa Môi trường", program: "Ngành KH Môi trường (CLC)" },
    "220": { faculty: "Khoa Môi trường", program: "Ngành CNKT Môi trường" },
    "290": { faculty: "Khoa Môi trường", program: "Ngành QLTN & Môi trường" },

    // 1xx & 2xx - Khoa Địa chất
    "160": { faculty: "Khoa Địa chất", program: "Ngành Địa chất học" },
    "270": { faculty: "Khoa Địa chất", program: "Ngành Kỹ thuật Địa chất" },

    // 1xx & 2xx - Khoa KH & CN Vật liệu
    "190": { faculty: "Khoa KH & CN Vật liệu", program: "Ngành Khoa học Vật liệu" },
    "250": { faculty: "Khoa KH & CN Vật liệu", program: "Ngành Công nghệ Vật liệu" },

    // 2xx - Khoa Hải dương học
    "210": { faculty: "Khoa Hải dương học", program: "Ngành Hải dương học" },
};

export const getUserRoleFromStudentID = (studentID?: string | null): string => {
    if (!studentID || typeof studentID !== "string") {
        return "Khách";
    }

    const cleanID = studentID.trim();
    if (cleanID.length < 4) {
        return "Khách";
    }

    // 2 số đầu tiên là niên khóa (ví dụ 21 -> K21)
    const cohort = cleanID.slice(0, 2);
    if (!/^\d{2}$/.test(cohort)) {
        return "Khách";
    }

    // Kiểm tra 3 số tiếp theo (vị trí index 2, 3, 4)
    const code3 = cleanID.slice(2, 5);
    if (FACULTY_PROGRAM_MAPPING[code3]) {
        const { faculty, program } = FACULTY_PROGRAM_MAPPING[code3];
        return `Sinh viên K${cohort} - ${faculty} - ${program}`;
    }

    // Kiểm tra 2 số tiếp theo cho hệ Cao đẳng (vị trí index 2, 3: 60 - 65)
    const code2 = cleanID.slice(2, 4);
    if (FACULTY_PROGRAM_MAPPING[code2]) {
        const { faculty, program } = FACULTY_PROGRAM_MAPPING[code2];
        return `Sinh viên K${cohort} - ${faculty} - ${program}`;
    }

    return `Sinh viên K${cohort}`;
};
