export const extractEmail = (email: string): string => {
    const match = email.match(/^([a-zA-Z]+)(\d{2})/);

    let email_processed = email; 

    if (match) {
        const letters = match[1]; 
        const firstTwoDigits = match[2]; 
        
        const domain = email.substring(email.indexOf('@')); 
        
        email_processed = `${letters}${firstTwoDigits}${domain}`;
    }
    return email_processed
}
export const extractStudentID = (email: string): string | undefined => {
    const parts = email.split("@");
    const prefix = parts[0];

    let studentId = undefined;

    if (prefix && !isNaN(Number(prefix))) {
        studentId = prefix;      
    }
    return studentId
}