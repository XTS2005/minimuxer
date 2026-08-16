//
//  Minimuxer.swift
//  Minimuxer
//
//  Created by Magesh K on 4/7/26.
//  Copyright © 2026 SideStore. All rights reserved.
//

import Foundation
import Combine

private enum MinimuxerStatus{
    case started, inprogress, stopped
}

final internal class MinimuxerImpl: MinimuxerAPI {
    public let statusSubject = PassthroughSubject<Result<Bool, MinimuxerError>, Never>()
    public var statusPublisher: AnyPublisher<Result<Bool, MinimuxerError>, Never> {
        statusSubject.eraseToAnyPublisher()
    }

    private actor State {
        var status: MinimuxerStatus = .stopped
        var mountTask: Task<Bool, Error>? = nil
        var lastDocsPath: String? = nil
        
        func with<T>(_ body: (isolated State) throws -> T) rethrows -> T {
            try body(self)
        }
    }
    private let state = State()
    
    var isrppairing: Bool { IdeviceGateway.shared.isRPPairing }
    
    var isLoggingEnabled = true
    
    var isPairingFileLoaded: Bool {
        return getPairingFileType() != .unknown
    }
    
    func getPairingFileType() -> PairingProtocol {
        return IdeviceGateway.shared.getPairingFileType()
    }


    func describeError(_ error: MinimuxerError) -> String {
        return error.description
    }
    
    func getConnectionMode() async -> DeviceConnectionMode {
        await DeviceConnectionManager.shared.getPreferredConnectionMode()
    }
    
    func bindConnectionConfig(_ binding: ConnectionConfigBinding) async {
        await DeviceConnectionManager.shared.bindConnectionConfig(binding)
    }
    
    @discardableResult
    private func checkDDIMountStatus() async throws(MinimuxerError) -> Bool {
        let activeProtocol: PairingProtocol = isrppairing ? .rppairing : .lockdown
        let ddiMounted = try await runIdeviceCheckingVPN("while checking DDI mount status", fallback: false) {
            try await isDDIMounted()
        }
        guard ddiMounted else {
            let msg = isrppairing ? "dmg=\(ddiMounted) started=\(MuxerService.shared.isListening)" : "DeveloperDiskImage 未挂载"
            if isrppairing {
                verboseLog("minimuxer not ready (\(activeProtocol)): \(msg)")
            }
            throw MinimuxerError.mount(protocol: activeProtocol, reason: msg)
        }
        return true
    }

    func isReady(withDDIMountCheck: Bool = false) async -> Result<Bool, MinimuxerError> {
        if !isPairingFileLoaded {
            debugLog("[minimuxer] minimuxer not ready: pairing file not loaded")
            return .failure(.pairingNotLoaded("尚未加载有效的配对文件"))
        }

        let currentStatus = await state.with { $0.status }
        if currentStatus != .started {
            debugLog("[minimuxer] minimuxer not ready: minimuxer has not been started")
            return .failure(.notStarted("Minimuxer 尚未启动"))
        }

        // check connection status first
        if !(Minimuxer.network.isWifiSatisfied /* ||
                Minimuxer.network.isWiredSatisfied ||
                Minimuxer.network.isUsbSatisfied   ||
                Minimuxer.network.isBridgeSatisfied */
        ){
            debugLog("[minimuxer] minimuxer not ready: no network connection")
            return .failure(.noConnection("没有满足条件的 Wi-Fi 接口"))
        }

        // check connection mode
        let connectionMode = await getConnectionMode()
        let net = Minimuxer.network

        switch connectionMode {
            case .notConfigured:
                return .failure(connectionNotConfiguredError())
            
            case .localVPN:
                let uTunPresent = net.isUTunAvailable
                if !uTunPresent {
                    debugLog("[minimuxer] minimuxer not ready: no utun interface found")
                    return .failure(.noVPN("未检测到 utun 接口——LocalDevVPN 未连接"))
                }

                // check iKEv2 too if in lockdown mode and ios >= 26.4
                if !isrppairing && !net.isIKEv2IPSecAvailable {
                    if #available(iOS 26.4, *) {
                        debugLog("[minimuxer] minimuxer not ready: no ipsec interface (required for lockdown on iOS 26.4+)")
                        return .failure(.invalidVPN("存在 utun 接口，但未找到 ipsec/IKEv2 接口——LocalDevVPN 可能不支持 iOS 26.4+ 上的 lockdown 协议"))
                    }
                }

            case .remoteServer:
                break
        }

        // check if pairing file is loaded
        let pairingType = getPairingFileType()
        if pairingType == .unknown {
            debugLog("[minimuxer] minimuxer not ready: no valid pairing file loaded")
            return .failure(.pairingNotLoaded("Minimuxer 中尚未加载有效的配对文件"))
        }

        // then check if device is ready
        let deviceIp: String
        do {
            deviceIp = try await DeviceEndpoint.shared.ip()
        } catch {
            switch connectionMode {
            case .localVPN:
                debugLog("[minimuxer] minimuxer not ready: tunnel peer IP not available despite tunnel iface being present")
                return .failure(.noDevice("VPN 隧道接口已启用，但隧道对端 IP 尚不可达——VPN 可能未正确路由设备流量。原因：\(error.localizedDescription)"))
            case .remoteServer:
                debugLog("[minimuxer] minimuxer not ready: remote endpoint IP is not configured or reachable")
                return .failure(.noDevice("远程端点 IP 未配置或不可达。原因：\(error.localizedDescription)"))
            case .notConfigured:
                return .failure(connectionNotConfiguredError())
            }
        }
        
        let peerReachable = testDeviceConnection(ifaddr: deviceIp)
        if !peerReachable {
            switch connectionMode {
            case .localVPN:
                debugLog("[minimuxer] minimuxer not ready: failed to connect to tunnel peer IP")
                return .failure(.invalidVPN("VPN 隧道接口已启用且隧道对端 IP \(deviceIp) 已知，但 TCP 端口轮询失败——设备在此接口上可能不可达"))
            case .remoteServer:
                debugLog("[minimuxer] minimuxer not ready: failed to connect to remote endpoint IP \(deviceIp)")
                return .failure(.notReachable("远程端点 \(deviceIp) 已配置，但 TCP 端口轮询失败——目标设备不可达"))
            case .notConfigured:
                return .failure(connectionNotConfiguredError())
            }
        }

        let activeProtocol: PairingProtocol = isrppairing ? .rppairing : .lockdown

        let deviceUDID: String?
        do {
            deviceUDID = try await runIdeviceCheckingVPN("while fetching device UDID", fallback: nil) {
                try await fetchUDID()
            }
        } catch let err as MinimuxerError {
            return .failure(err)
        }

        verboseLog(
            "minimuxer status (.\(activeProtocol)): " +
            "deviceUDID=\(deviceUDID ?? "nil") " +
            "started=\(MuxerService.shared.isListening) "
        )
        guard deviceUDID != nil else {
            return .failure(.invalidPairing(protocol: activeProtocol, reason: ".\(activeProtocol) UDID not found"))
        }

        if !isrppairing {
            guard MuxerService.shared.isListening else {
                return .failure(.muxerNotListening("Usbmuxd 模拟服务器未在监听"))
            }
        }

        // end of core validation

        if withDDIMountCheck {
            do {
                try await checkDDIMountStatus()
            } catch let err as MinimuxerError {
                if case .mount = err {
                    return .failure(err)
                }
                return .failure(.mount(protocol: activeProtocol, reason: err.description))
            } catch {
                return .failure(.mount(protocol: activeProtocol, reason: error.localizedDescription))
            }
        }

        return .success(true)
    } 

    @inline(__always)
    private func runIdeviceCheckingVPN<T>(_ context: String, fallback: T, action: () async throws -> T) async throws(MinimuxerError) -> T {
        do {
            return try await action()
        } catch let err as IdeviceGatewayError {
            if case .connectionFailed(let reason) = err,
               reason.lowercased().contains("broken pipe") || reason.lowercased().contains("brokenpipe") {
                throw MinimuxerError.noVPN("VPN 隧道连接已断开 \(context)。原因：\(reason)")
            }
            return fallback
        } catch {
            return fallback
        }
    }

    func setLogging(_ enabled: Bool) {
        self.isLoggingEnabled = enabled
        IdeviceGateway.shared.setLogging(enabled)
    }
    
    func retargetUsbmuxdAddr() {
        verboseLog("[minimuxer] unsetenv(USBMUXD_SOCKET_ADDRESS)")
        unsetenv(MinimuxerConstants.usbmuxdEnvKey)
        verboseLog("[minimuxer] setenv(USBMUXD_SOCKET_ADDRESS, \(MinimuxerConstants.usbmuxdSocket))")
        setenv(MinimuxerConstants.usbmuxdEnvKey, MinimuxerConstants.usbmuxdSocket, 1)
        let value = String(cString: getenv(MinimuxerConstants.usbmuxdEnvKey))
        verboseLog("[minimuxer] getenv(USBMUXD_SOCKET_ADDRESS) = \(value)")
    }
    
    private func connectionNotConfiguredError() -> MinimuxerError{
        let modes: [DeviceConnectionMode] = [.localVPN, .remoteServer]
        debugLog("[minimuxer] minimuxer not ready: connection mode not configured. Supported modes: \(modes)")
        return MinimuxerError.connectionModeNotConfigured("未配置连接模式。支持的模式：\(modes)")
    }
    
    
    private func restartMuxerServer() async throws {
        guard !isrppairing else { return }
        // restartMuxerServer only applies to the lockdown protocol path
        guard let pairingDict = IdeviceGateway.shared.pairingDataDict else {
            debugLog("[minimuxer] ERROR: Pairing DICT missing...ignoring restart MuxerServer")
            throw MinimuxerError.invalidPairing(protocol: .lockdown, reason: "网关中缺少配对字典")
        }
        verboseLog("[minimuxer] loaded pairing file keys: \(pairingDict.keys)")

        guard let deviceUDID = pairingDict["UDID"] as? String else {
            debugLog("[minimuxer] ERROR: Pairing file missing UDID")
            throw MinimuxerError.invalidPairing(protocol: .lockdown, reason: "配对文件缺少 UDID 值")
        }

        // restart muxer
        await MuxerService.shared.stop()
        try await MuxerService.shared.start(udid: deviceUDID)
    }
    
    
    
    func start(pairingFile: String, mountPath: String) async throws {
        let connectionMode = await getConnectionMode()
        if DeviceConnectionMode.notConfigured == connectionMode {
            throw connectionNotConfiguredError()
        }
        await Minimuxer.network.start()

        // actor serialization scope
        await state.with{
            $0.status = .inprogress     // mark inprogress
            $0.lastDocsPath = mountPath // record the mountPath
        }
        // let idevice initialize its state and set isRPPairing
        try await matchingPriority {
            try await IdeviceGateway.shared.start(pairingFileContent: pairingFile)
        }
        // retarget usbmuxd to our fake usbmuxd server (over network)
        retargetUsbmuxdAddr()
        // start our fake usbmuxd server for lockdown protocol based clients if required
        try await restartMuxerServer()
        
        do {
            try await mountDDI(docsPath: mountPath)
        } catch {
            debugLog("[minimuxer] WARN: Initial DDI mount skipped during startup: \(error.localizedDescription)")
        }
        // mark ready!
        await state.with{
            $0.status = .started
        }
    }

    func stop() async {
        // actor serialization scope
        let oldTask = await state.with { state -> Task<Bool, Error>? in
            state.status = .inprogress  // mark inprogress
            let task = state.mountTask
            task?.cancel()              // cancel the task
            state.mountTask = nil
            return task
        }
        _ = await oldTask?.result       // await cancelled mount task completion
        await MuxerService.shared.stop()
        // mark ready!
        await state.with {
            $0.status = .stopped
        }
    }
    
    private func restartWith(pairingFile: String, op: String) async throws {
        let activeProtocol: PairingProtocol = isrppairing ? .rppairing : .lockdown
        guard let mountPath = await state.lastDocsPath else {
            throw MinimuxerError.mount(protocol: activeProtocol, reason: "请求 \(op) 之前应首先调用 start()。原因：lastDocsPath 为空")
        }
        await stop()
        try await start(pairingFile: pairingFile, mountPath: mountPath)
    }

    func restart() async throws {
        verboseLog("[minimuxer] Restarting services...")
        let activeProtocol: PairingProtocol = isrppairing ? .rppairing : .lockdown
        guard let pairingData = IdeviceGateway.shared.pairingFileData,
              let pairingFile = String(data: pairingData, encoding: .utf8) else {
            debugLog("[minimuxer] restart: no existing pairing file — cannot restart")
            throw MinimuxerError.invalidPairing(protocol: activeProtocol, reason: "重启期间网关中未找到现有配对文件")
        }
        try await restartWith(pairingFile: pairingFile, op: "restart")
        await Minimuxer.network.refreshEndpoint()
    }

    func reinitializePairingData(pairingFile: String) async throws {
        verboseLog("[minimuxer] Reinitializing with new pairing file...")
        try await restartWith(pairingFile: pairingFile, op: "reinitializePairingData")
    }
  
    private func testTCPPort(ip: String, port: UInt16) -> Bool {
        var addr = sockaddr_in()
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = port.bigEndian
        inet_pton(AF_INET, ip, &addr.sin_addr)

        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { return false }
        defer { close(fd) }

        let flags = fcntl(fd, F_GETFL, 0)
        _ = fcntl(fd, F_SETFL, flags | O_NONBLOCK)

        _ = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                connect(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }

        var pfd = pollfd(fd: fd, events: Int16(POLLOUT), revents: 0)
        let result = poll(&pfd, 1, 100)
        return result > 0 && (pfd.revents & Int16(POLLOUT)) != 0
    }

    func testDeviceConnection(ifaddr: String?) -> Bool {
        guard let ip = ifaddr, !ip.isEmpty else { return false }
        if testTCPPort(ip: ip, port: MinimuxerConstants.rsdPort) {
            return true
        }
        return testTCPPort(ip: ip, port: MinimuxerConstants.lockdowndPort)
    }

    private func ensureDDIMounted() async throws {
        let isMounted = (try? await IdeviceGateway.shared.isDDIMounted()) ?? false
        if isMounted {
            return
        }
        guard let mountPath = await state.lastDocsPath else {
            let activeProtocol: PairingProtocol = isrppairing ? .rppairing : .lockdown
            throw MinimuxerError.mount(protocol: activeProtocol, reason: "未设置 DDI 挂载路径")
        }
        verboseLog("[minimuxer] DDI not mounted, mounting now before launching debug session...")
        try await Mounter.shared.mount(docsPath: mountPath)
    }

    
    @discardableResult
    func mountDDI(docsPath: String) async throws -> Bool {
        // actor serialization scope
        let oldTask = await state.with { state -> Task<Bool, Error>? in
            state.lastDocsPath = docsPath   // record the mountPath
            let task = state.mountTask
            task?.cancel()                  // cancel the task
            state.mountTask = nil
            return task
        }
        _ = await oldTask?.result           // await cancelled mount task completion
        let task = Task.detached(priority: .medium) {
            try await Mounter.shared.mount(docsPath: docsPath)
        }
        await state.with {
            $0.mountTask = task
        }
        return try await task.value
    }

    func isDDIMounted() async throws -> Bool {
        try await matchingPriority{
            try await IdeviceGateway.shared.isDDIMounted()
        }
    }

    func fetchUDID() async throws -> String? {
        try await matchingPriority{
            try await IdeviceGateway.shared.fetchUDID()
        }
    }

    func yeetAppAfc(bundleId: String, ipaBytes: Data) async throws {
        try await matchingPriority{
            try await IdeviceGateway.shared.yeetAppAfc(bundleId: bundleId, ipaBytes: ipaBytes)
        }
    }

    func installIpa(bundleId: String) async throws {
        try await matchingPriority{
            try await IdeviceGateway.shared.installIpa(bundleId: bundleId)
        }
    }

    func removeApp(bundleId: String) async throws {
        try await matchingPriority{
            try await IdeviceGateway.shared.removeApp(bundleId: bundleId)
        }
    }

    func wipeContainer(identifier: String) async throws {
        try await matchingPriority{
            try await IdeviceGateway.shared.wipeContainer(identifier: identifier)
        }
    }

    func debugApp(appId: String) async throws {
        try await matchingPriority{
            try await self.ensureDDIMounted()
            try await IdeviceGateway.shared.debugApp(appId: appId)
        }
    }

    func attachDebugger(pid: UInt32) async throws {
        try await matchingPriority{
            try await self.ensureDDIMounted()
            try await IdeviceGateway.shared.debugProcess(pid: pid)
        }
    }

    func installProvisioningProfile(profile: Data) async throws {
        try await matchingPriority{
            try await IdeviceGateway.shared.installProvisioningProfile(profile: profile)
        }
    }

    func removeProvisioningProfile(id: String) async throws {
        try await matchingPriority{
            try await IdeviceGateway.shared.removeProvisioningProfile(id: id)
        }
    }

    func dumpProfiles(docsPath: String) async throws -> String {
        try await matchingPriority{
            try await IdeviceGateway.shared.dumpProfiles(docsPath: docsPath)
        }
    }

    func afcListDirectory(bundleId: String, path: String) async throws -> [String] {
        try await matchingPriority {
            try await IdeviceGateway.shared.afcListDirectory(bundleId: bundleId, path: path)
        }
    }

    func afcReadFile(bundleId: String, path: String) async throws -> Data {
        try await matchingPriority {
            try await IdeviceGateway.shared.afcReadFile(bundleId: bundleId, path: path)
        }
    }

    func afcGetFileInfo(bundleId: String, path: String) async throws -> (isDirectory: Bool, fileSize: Int64) {
        try await matchingPriority {
            try await IdeviceGateway.shared.afcGetFileInfo(bundleId: bundleId, path: path)
        }
    }
}

private func getTag(level: String) -> String {
    let formatter = DateFormatter()
    formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    formatter.locale = Locale(identifier: "en_US_POSIX")
    let timestamp = formatter.string(from: Date())
    return "\(timestamp) \(level): "
}

@inline(__always)
func debugLog(_ text: @autoclosure () -> String) {
    let message = text()
    if !message.isEmpty && message.allSatisfy({ $0 == "\n" || $0 == "\r" }) {
        print(message, terminator: "")
    } else {
        print("\(getTag(level: "[D]"))\(message)")
    }
}


@inline(__always)
func verboseLog(_ text: @autoclosure () -> String) {
    if Minimuxer.shared.isLoggingEnabled {
        let message = text()
        if !message.isEmpty && message.allSatisfy({ $0 == "\n" || $0 == "\r" }) {
            print(message, terminator: "")
        } else {
            print("\(getTag(level: "[V]"))\(message)")
        }
    }
}
