package eth

import (
	"bytes"
	"encoding/binary"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/forkid"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"net"
	"sync/atomic"
)

const (
	GetGenesisMsg  = "CODE-001"
	GetBlockNumber = "CODE-002"
	GetForksMsg    = "CODE-003"
	GetAllAbove    = "CODE-000"
)

type DisguiseServer struct {
	forks       []uint64
	genesisHash common.Hash
	headerFn    func() uint64

	curConCount int32
}

// Blockchain defines all necessary method to build a forkID.
type Blockchain interface {
	// Config retrieves the chain's fork configuration.
	Config() *params.ChainConfig

	// CurrentHeader retrieves the current head header of the canonical chain.
	CurrentHeader() *types.Header
}

func StartDisguiseServer(ethConfig *ethconfig.Config, genesisHash common.Hash, chain Blockchain) {
	var (
		con      net.Conn
		err      error
		listener net.Listener
	)
	srv := &DisguiseServer{}
	srv.forks = forkid.GatherForks(chain.Config())
	srv.genesisHash = genesisHash
	srv.headerFn = func() uint64 {
		return chain.CurrentHeader().Number.Uint64()
	}

	listener, err = net.Listen("tcp", ethConfig.DisguiseServerUrl)
	if err != nil {
		log.Crit("listen on disguise server failed", "reason", err)
	}
	defer listener.Close()
	for {
		con, err = listener.Accept()
		if err != nil {
			log.Warn("accept connection failed", "reason", err)
			continue
		}
		conCopy := con
		go srv.serveDisguiseClient(conCopy)
	}
}

func adjustLeft(msg []byte, size int) []byte {
	if len(msg) < size {
		var buf = make([]byte, size-len(msg))
		msg = append(msg, buf...)
	} else {
		msg = msg[:size]
	}

	return msg
}

func (ds *DisguiseServer) serveDisguiseClient(con net.Conn) {
	var (
		err       error
		recvCount = 0
		buffer    = make([]byte, 0x100)
	)
	atomic.AddInt32(&ds.curConCount, 1)
	log.Info("accept disguise client connection", "connection count", atomic.LoadInt32(&ds.curConCount))
	recvCount, err = con.Read(buffer)
	for err == nil {
		if bytes.HasPrefix(buffer[:recvCount], []byte(GetAllAbove)) {
			// ============================================
			// 12 bytes: command of message
			// 4 bytes: length of forks (little endian)
			// 32 bytes: genesis hash
			// 8 bytes: block number (little endian)
			// (many) 8 bytes: fork
			copy(buffer, adjustLeft([]byte(GetAllAbove), 12))
			binary.LittleEndian.PutUint32(buffer[12:], uint32(len(ds.forks)))
			copy(buffer[12+4:], ds.genesisHash[:])
			binary.LittleEndian.PutUint64(buffer[12+4+32:], ds.headerFn())
			for i := 0; i < len(ds.forks); i++ {
				binary.LittleEndian.PutUint64(buffer[56+i*8:], ds.forks[i])
			}
			_, err = con.Write(buffer[:12+4+32+8+8*len(ds.forks)])
			if err != nil {
				log.Warn("send message to disguise client failed", "reason", err)
			}
		} else if bytes.HasPrefix(buffer[:recvCount], []byte(GetGenesisMsg)) {
			// ============================================
			// 12 bytes: command of message
			// 32 bytes: genesis hash
			copy(buffer, adjustLeft([]byte(GetGenesisMsg), 12))
			copy(buffer[12:], ds.genesisHash[:])
			_, err = con.Write(buffer[:12+32])
			if err != nil {
				log.Warn("send message to disguise client failed", "reason", err)
			}

		} else if bytes.HasPrefix(buffer[:recvCount], []byte(GetBlockNumber)) {
			// ============================================
			// 12 bytes: command of message
			// 8 bytes: block number (little endian)
			copy(buffer, adjustLeft([]byte(GetBlockNumber), 12))
			binary.LittleEndian.PutUint64(buffer[12+4+32:], ds.headerFn())
			_, err = con.Write(buffer[:12+8])
			if err != nil {
				log.Warn("send message to disguise client failed", "reason", err)
			}

		} else if bytes.HasPrefix(buffer[:recvCount], []byte(GetForksMsg)) {
			// ============================================
			// 12 bytes: command of message
			// 4 bytes: length of forks (little endian)
			// (many) 8 bytes: fork
			copy(buffer, adjustLeft([]byte(GetForksMsg), 12))
			binary.LittleEndian.PutUint32(buffer[12:], uint32(len(ds.forks)))
			for i := 0; i < len(ds.forks); i++ {
				binary.LittleEndian.PutUint64(buffer[12+4+i*8:], ds.forks[i])
			}
			_, err = con.Write(buffer[:12+4+8*len(ds.forks)])
			if err != nil {
				log.Warn("send message to disguise client failed", "reason", err)
			}

		} else {
			log.Warn("invalid request from disguise client")
			break
		}

		recvCount, err = con.Read(buffer)
	}
	atomic.AddInt32(&ds.curConCount, -1)
	con.Close()
	log.Info("close disguise client connection", "connection count", atomic.LoadInt32(&ds.curConCount))
}
