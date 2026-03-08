// crypto/crypto.js

export async function generateKeyPair() {
    return await window.crypto.subtle.generateKey(
        { name: "ECDH", namedCurve: "P-256" },
        true,
        ["deriveKey"]
    );
}

export async function exportPublicKey(key) {
    const exported = await window.crypto.subtle.exportKey("spki", key);
    return btoa(String.fromCharCode(...new Uint8Array(exported)));
}

export async function importPublicKey(pem) {
    const binaryDerString = atob(pem);
    const binaryDer = new Uint8Array(binaryDerString.length);
    for (let i = 0; i < binaryDerString.length; i++) {
        binaryDer[i] = binaryDerString.charCodeAt(i);
    }
    return await window.crypto.subtle.importKey(
        "spki",
        binaryDer.buffer,
        { name: "ECDH", namedCurve: "P-256" },
        true,
        []
    );
}

export async function deriveAES(privateKey, publicKey) {
    return await window.crypto.subtle.deriveKey(
        { name: "ECDH", public: publicKey },
        privateKey,
        { name: "AES-GCM", length: 256 },
        true,
        ["encrypt", "decrypt"]
    );
}

export async function encrypt(key, text) {
    const encoder = new TextEncoder();
    const iv = window.crypto.getRandomValues(new Uint8Array(12));
    const encrypted = await window.crypto.subtle.encrypt(
        { name: "AES-GCM", iv: iv },
        key,
        encoder.encode(text)
    );
    // Склеиваем IV и шифр для передачи
    const combined = new Uint8Array(iv.length + encrypted.byteLength);
    combined.set(iv);
    combined.set(new Uint8Array(encrypted), iv.length);
    return btoa(String.fromCharCode(...combined));
}

export async function decrypt(key, b64) {
    const combined = new Uint8Array(atob(b64).split("").map(c => c.charCodeAt(0)));
    const iv = combined.slice(0, 12);
    const data = combined.slice(12);
    const decrypted = await window.crypto.subtle.decrypt(
        { name: "AES-GCM", iv: iv },
        key,
        data
    );
    return new TextDecoder().decode(decrypted);
}

// Функция для импорта публичного ключа именно для шифрования (RSA/OAEP)
// Примечание: Если твои ключи только ECDH, нам нужно использовать AES-обертку.
// Для простоты используем импорт ключа для шифрования данных.
export async function decryptGroupKey(privKeyOrB64, ownerPubKey, encryptedData) {
    let myPrivKey;
    
    // Проверяем: это уже объект ключа или еще строка Base64?
    if (privKeyOrB64 instanceof CryptoKey) {
        myPrivKey = privKeyOrB64;
    } else {
        myPrivKey = await importPrivateKey(privKeyOrB64);
    }
    
    // Генерируем общий секрет (ECDH)
    const sharedAES = await deriveAES(myPrivKey, ownerPubKey);
    
    // Расшифровываем ключ группы этим секретом
    return await decrypt(sharedAES, encryptedData);
}


export async function encryptGroupKey(pubKeyPem, groupKeyB64, providedPrivKey = null) {
    const pubKey = await importPublicKey(pubKeyPem);
    
    // Если ключ передан напрямую (из IndexedDB), используем его. 
    // Если нет — ищем в localStorage (для совместимости)
    let myPrivKey = providedPrivKey;
    if (!myPrivKey) {
        const myPrivKeyB64 = localStorage.getItem('user_private_key');
        if (!myPrivKeyB64) throw new Error("Собственный приватный ключ не найден");
        myPrivKey = await importPrivateKey(myPrivKeyB64);
    }
    
    const sharedAES = await deriveAES(myPrivKey, pubKey);
    return await encrypt(sharedAES, groupKeyB64);
}

export async function importPrivateKey(b64) {
    try {
        const binaryDerString = atob(b64);
        const binaryDer = new Uint8Array(binaryDerString.length);
        for (let i = 0; i < binaryDerString.length; i++) {
            binaryDer[i] = binaryDerString.charCodeAt(i);
        }
        return await window.crypto.subtle.importKey(
            "pkcs8", // Убедитесь, что здесь pkcs8, а не spki
            binaryDer.buffer,
            { name: "ECDH", namedCurve: "P-256" },
            true,
            ["deriveKey"]
        );
    } catch (e) {
        console.error("Ошибка формата приватного ключа:", e);
        throw e;
    }
}

export async function importAESKey(base64) {
    const raw = Uint8Array.from(atob(base64), c => c.charCodeAt(0));
    return await window.crypto.subtle.importKey("raw", raw, "AES-GCM", true, ["encrypt", "decrypt"]);
}




// Алиасы для совместимости с разными частями кода
export const generateECDHKeyPair = generateKeyPair;
export const exportECDHPublicKey = exportPublicKey;
export const importECDHPublicKey = importPublicKey;
export const deriveAESKey = deriveAES;

// Экспорт приватного ключа в JWK формат
export async function exportECDHPrivateKey(privateKey) {
    return await window.crypto.subtle.exportKey("jwk", privateKey);
}

// Хеширование пароля для хранения
export async function hashPassword(password) {
    const encoder = new TextEncoder();
    const data = encoder.encode(password);
    const hashBuffer = await window.crypto.subtle.digest("SHA-256", data);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

// Шифрование данных паролем (для хранения приватного ключа)
export async function encryptWithPassword(password, data) {
    const encoder = new TextEncoder();
    const salt = window.crypto.getRandomValues(new Uint8Array(16));
    
    // Получаем ключ из пароля через PBKDF2
    const keyMaterial = await window.crypto.subtle.importKey(
        "raw",
        encoder.encode(password),
        "PBKDF2",
        false,
        ["deriveBits", "deriveKey"]
    );
    
    const key = await window.crypto.subtle.deriveKey(
        {
            name: "PBKDF2",
            salt: salt,
            iterations: 100000,
            hash: "SHA-256"
        },
        keyMaterial,
        { name: "AES-GCM", length: 256 },
        false,
        ["encrypt", "decrypt"]
    );
    
    const iv = window.crypto.getRandomValues(new Uint8Array(12));
    const encrypted = await window.crypto.subtle.encrypt(
        { name: "AES-GCM", iv: iv },
        key,
        encoder.encode(data)
    );
    
    // Комбинируем: salt (16) + iv (12) + encrypted
    const combined = new Uint8Array(salt.length + iv.length + encrypted.byteLength);
    combined.set(salt, 0);
    combined.set(iv, salt.length);
    combined.set(new Uint8Array(encrypted), salt.length + iv.length);
    
    return btoa(String.fromCharCode(...combined));
}

// Расшифровка данных паролем
export async function decryptWithPassword(password, encryptedB64) {
    const encoder = new TextEncoder();
    const combined = new Uint8Array(atob(encryptedB64).split("").map(c => c.charCodeAt(0)));
    
    const salt = combined.slice(0, 16);
    const iv = combined.slice(16, 28);
    const encrypted = combined.slice(28);
    
    const keyMaterial = await window.crypto.subtle.importKey(
        "raw",
        encoder.encode(password),
        "PBKDF2",
        false,
        ["deriveBits", "deriveKey"]
    );
    
    const key = await window.crypto.subtle.deriveKey(
        {
            name: "PBKDF2",
            salt: salt,
            iterations: 100000,
            hash: "SHA-256"
        },
        keyMaterial,
        { name: "AES-GCM", length: 256 },
        false,
        ["encrypt", "decrypt"]
    );
    
    const decrypted = await window.crypto.subtle.decrypt(
        { name: "AES-GCM", iv: iv },
        key,
        encrypted
    );
    
    return new TextDecoder().decode(decrypted);
}

// Упаковка AES ключа с помощью публичного ключа (для групп)
export async function wrapAESKey(publicKey, aesKey) {
    // Экспортируем AES ключ
    const rawKey = await window.crypto.subtle.exportKey("raw", aesKey);
    const keyB64 = btoa(String.fromCharCode(...new Uint8Array(rawKey)));
    
    // Генерируем временную пару ключей для обмена
    const ephemeralKeyPair = await generateKeyPair();
    const sharedSecret = await deriveAES(ephemeralKeyPair.privateKey, publicKey);
    
    // Шифруем AES ключ
    const encrypted = await encrypt(sharedSecret, keyB64);
    
    // Экспортируем публичный эфемерный ключ
    const ephemeralPubKey = await exportPublicKey(ephemeralKeyPair.publicKey);
    
    // Возвращаем: эфемерный публичный ключ + зашифрованные данные
    return JSON.stringify({
        ephemeralPublicKey: ephemeralPubKey,
        encryptedKey: encrypted
    });
}

// Распаковка AES ключа с помощью приватного ключа
export async function unwrapAESKey(privateKey, wrappedKeyJSON) {
    const wrapped = JSON.parse(wrappedKeyJSON);
    
    // Импортируем эфемерный публичный ключ
    const ephemeralPubKey = await importPublicKey(wrapped.ephemeralPublicKey);
    
    // Получаем общий секрет
    const sharedSecret = await deriveAES(privateKey, ephemeralPubKey);
    
    // Расшифровываем
    const keyB64 = await decrypt(sharedSecret, wrapped.encryptedKey);
    
    // Импортируем AES ключ
    const rawKey = new Uint8Array(atob(keyB64).split("").map(c => c.charCodeAt(0)));
    return await window.crypto.subtle.importKey(
        "raw",
        rawKey,
        { name: "AES-GCM" },
        true,
        ["encrypt", "decrypt"]
    );
}