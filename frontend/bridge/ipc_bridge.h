#ifndef PEWPAW_IPC_BRIDGE_H
#define PEWPAW_IPC_BRIDGE_H

#include <QObject>
#include <QTcpSocket>
#include <QJsonObject>
#include <QJsonDocument>

#define LISTEN_ADDR "127.0.0.1"
#define LISTEN_PORT 49152

class IpcBridge : public QObject
{
    Q_OBJECT

public:
    explicit IpcBridge(QObject *parent = nullptr);
    ~IpcBridge();

    Q_INVOKABLE void connectToBackend();
    Q_INVOKABLE void sendCommand(const QString &type, const QJsonObject &payload = QJsonObject());
    Q_INVOKABLE bool isConnected() const;

signals:
    void connected();
    void disconnected();
    void eventReceived(const QJsonObject &event);

private slots:
    void onConnected();
    void onDisconnected();
    void onReadyRead();
    void onError(QTcpSocket::SocketError error);

private:
    void processLine(const QByteArray &line);
    QTcpSocket *m_socket;
    QByteArray m_buffer;
};

#endif
