// 免费的 Cloudflare Workers VLESS 代理脚本
import { connect } from 'cloudflare:sockets';

// 1. 【必须修改】去任意 UUID 生成网站生成一个你自己的 UUID，替换掉下面这行，作为你的连接密码
const USER_ID = '35dfb718-ec37-4bf7-9f6b-7347a46ad6a5'; 

export default {
    async fetch(request, env, ctx) {
        try {
            const upgradeHeader = request.headers.get('Upgrade');
            if (!upgradeHeader || upgradeHeader !== 'websocket') {
                return new Response('Node is Running Successfully!', { status: 200 });
            }
            return await vlessOverWSHandler(request);
        } catch (err) {
            return new Response(err.toString(), { status: 500 });
        }
    },
};

async function vlessOverWSHandler(request) {
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);
    server.accept();

    let remoteSocketWrapper = null;
    let isVlessHeaderValid = false;

    client.addEventListener('message', async (event) => {
        try {
            const chunk = event.data;
            if (isVlessHeaderValid) {
                if (remoteSocketWrapper) {
                    const writer = remoteSocketWrapper.writable.getWriter();
                    await writer.write(chunk);
                    writer.releaseLock();
                }
                return;
            }

            // 解密 VLESS 协议头部
            const buffer = chunk;
            if (buffer.byteLength < 24) return;
            const version = new Uint8Array(buffer.slice(0, 1));
            const id = uuidStringify(new Uint8Array(buffer.slice(1, 17)));
            
            if (id !== USER_ID) {
                client.close();
                return;
            }

            isVlessHeaderValid = true;
            const optLength = new Uint8Array(buffer.slice(17, 18));
            const cmd = new Uint8Array(buffer.slice(18 + optLength, 19 + optLength));
            const port = new DataView(buffer.slice(19 + optLength, 21 + optLength)).getUint16(0);
            
            let addressIndex = 22 + optLength;
            const addressType = new Uint8Array(buffer.slice(21 + optLength, 22 + optLength));
            let address = '';

            if (addressType === 1) {
                address = new Uint8Array(buffer.slice(addressIndex, addressIndex + 4)).join('.');
                addressIndex += 4;
            } else if (addressType === 2) {
                const addressLength = new Uint8Array(buffer.slice(addressIndex, addressIndex + 1));
                addressIndex += 1;
                address = new TextDecoder().decode(buffer.slice(addressIndex, addressIndex + addressLength));
                addressIndex += addressLength;
            }

            if (cmd !== 1) {
                client.close();
                return;
            }

            // 建立远程TCP连接
            const remoteSocket = connect({ hostname: address, port: port });
            remoteSocketWrapper = remoteSocket;

            const rawClientData = buffer.slice(addressIndex);
            const writer = remoteSocket.writable.getWriter();
            await writer.write(rawClientData);
            writer.releaseLock();

            remoteSocket.readable.pipeTo(new WritableStream({
                async write(chunk) {
                    client.send(chunk);
                },
                close() {
                    client.close();
                }
            })).catch(() => client.close());

        } catch (error) {
            client.close();
        }
    });

    client.addEventListener('close', () => {
        if (remoteSocketWrapper) remoteSocketWrapper.close();
    });

    return new Response(null, { status: 101, webSocket: client });
}

function uuidStringify(buffer) {
    const byteToHex = [];
    for (let i = 0; i < 256; ++i) {
        byteToHex.push((i + 0x100).toString(16).slice(1));
    }
    return [
        byteToHex[buffer[0]], byteToHex[buffer[1]], byteToHex[buffer[2]], byteToHex[buffer[3]], '-',
        byteToHex[buffer[4]], byteToHex[buffer[5]], '-',
        byteToHex[buffer[6]], byteToHex[buffer[7]], '-',
        byteToHex[buffer[8]], byteToHex[buffer[9]], '-',
        byteToHex[buffer[10]], byteToHex[buffer[11]], byteToHex[buffer[12]], byteToHex[buffer[13]], byteToHex[buffer[14]], byteToHex[buffer[15]]
    ].join('');
}
