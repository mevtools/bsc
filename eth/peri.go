package eth

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"math"
	"math/big"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// PERI_AND_LATENCY_RECORDER_CODE_PIECE

const (
	milli2Nano                = 1000000
	transactionArrivalReplace = 30000
	enodeSplitIndex           = 137
	periNodeDbPath            = "peri_nodes"
	periNodeCountKey          = "nodes_count"
	periNodeKeyPrefix         = "n_"
	maxBlockDist              = 32 // Maximum allowed distance from the chain head to block received
)

type Peri struct {
	config           *ethconfig.Config // ethconfig used globally during program execution
	handler          *handler          // implement handler to blocks and transactions arriving
	replaceCount     int               // count of replacement during every period
	maxDelayDuration int64             // max delay duration in nano time
	announcePenalty  int64

	approachingMiners bool

	locker           *sync.Mutex                      // locker to protect the below map fields.
	txArrivals       map[common.Hash]int64            // record whether transactions are received, announcement and body are treated equally
	txArrivalPerPeer map[common.Hash]map[string]int64 // record transactions timestamp by peers
	txOldArrivals    map[common.Hash]int64            // record all stale transactions, avoid the situation where the message from the straggler is recorded

	blockArrivals       map[blockAnnounce]int64
	blockArrivalPerPeer map[blockAnnounce]map[string]int64

	peersSnapShot map[string]string // record peers id to enode, used by noDropPeer function
	blacklist     map[string]bool   // black list of ip address

	fileLogger log.Logger // eviction log
	nodesDb    *enode.DB
}

type idScore struct {
	id    string
	score float64
}

type blockAnnounce struct {
	hash   common.Hash
	number uint64
}

func blockAnnouncesFromHashesAndNumbers(hashes []common.Hash, numbers []uint64) []blockAnnounce {
	var length int
	if len(hashes) <= len(numbers) {
		length = len(hashes)
	} else {
		length = len(numbers)
	}

	var result = make([]blockAnnounce, 0, length)
	for i := 0; i < length; i++ {
		result = append(result, blockAnnounce{
			hash:   hashes[i],
			number: numbers[i],
		})
	}

	return result
}

func CreatePeri(p2pServe *p2p.Server, config *ethconfig.Config, h *handler) *Peri {
	var (
		err   error
		f     *os.File
		node  *enode.Node
		nodes []*enode.Node
	)
	peri := &Peri{
		config:            config,
		handler:           h,
		locker:            new(sync.Mutex),
		replaceCount:      int(math.Round(float64(h.maxPeers) * config.PeriReplaceRatio)),
		maxDelayDuration:  int64(config.PeriMaxDelayPenalty * milli2Nano),
		announcePenalty:   int64(config.PeriAnnouncePenalty * milli2Nano),
		approachingMiners: config.PeriApproachMiners,
		txArrivals:        make(map[common.Hash]int64),
		txArrivalPerPeer:  make(map[common.Hash]map[string]int64),
		txOldArrivals:     make(map[common.Hash]int64),

		blockArrivals:       make(map[blockAnnounce]int64),
		blockArrivalPerPeer: make(map[blockAnnounce]map[string]int64),

		peersSnapShot: make(map[string]string),
		blacklist:     make(map[string]bool),
		fileLogger:    log.New(),
	}

	databasePath := filepath.Join(config.PeriDataDirectory, periNodeDbPath)
	peri.nodesDb, err = enode.OpenDB(databasePath)
	if err != nil {
		log.Crit("open peri database failed", "err", err)
	}

	periNodesCount := peri.nodesDb.FetchUint64([]byte(periNodeCountKey))
	if periNodesCount > 0 {
		for i := 0; i < int(periNodesCount); i++ {
			enodeUrl := peri.nodesDb.FetchString([]byte(periNodeKeyPrefix + strconv.Itoa(i)))
			if enodeUrl != "" {
				node, err = enode.Parse(enode.ValidSchemes, enodeUrl)
				if err != nil {
					log.Warn("parse enode failed when create peri", "err", err, "url", enodeUrl)
					continue
				}
				nodes = append(nodes, node)
			}
		}
	}
	p2pServe.AddPeriInitialNodes(nodes)

	if config.PeriLogFilePath != "" {
		f, err = os.OpenFile(config.PeriLogFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Crit("open peri log file failed", "err", err)
		}
		logHandler := log.StreamHandler(f, log.LogfmtFormat())
		peri.fileLogger.SetHandler(logHandler)
	}

	return peri
}

// StartPeri Start Peri (at the initialization of geth)
func (p *Peri) StartPeri() {
	go func() {
		var (
			interrupt       = make(chan os.Signal, 1)
			killed          = false
			saveNodesOnExit = false
			err             error
		)
		signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(interrupt)
		defer p.nodesDb.Close()

		ticker := time.NewTicker(time.Second * time.Duration(p.config.PeriPeriod))
		for killed == false {
			select {
			case <-ticker.C:
				log.Warn("new peri period start disconnect by score")
				p.disconnectByScore()
				saveNodesOnExit = true
			case <-interrupt:
				log.Warn("peri eviction policy interrupted")
				killed = true
			}
		}

		if saveNodesOnExit {
			scores, _ := p.getScores()
			numDrop := p.replaceCount + len(scores) - p.handler.maxPeers
			if numDrop < 0 {
				numDrop = 0
			}
			scores = scores[numDrop:]
			err = p.nodesDb.StoreInt64([]byte(periNodeCountKey), int64(len(scores)))
			if err != nil {
				log.Warn("peri store node count failed when exit", "err", err)
			}
			for i, element := range scores {
				enode := p.peersSnapShot[element.id]
				err = p.nodesDb.StoreString([]byte(periNodeKeyPrefix+strconv.Itoa(i)), enode)
				if err != nil {
					log.Warn("peri store enode failed when exit", "err", err)
				}
			}
		}
	}()
}

func (p *Peri) lock() {
	p.locker.Lock()
}

func (p *Peri) unlock() {
	p.locker.Unlock()
}

func (p *Peri) recordBlockAnnounces(peer *eth.Peer, hashes []common.Hash, numbers []uint64, isAnnouncement bool) {
	var (
		timestamp             = time.Now().UnixNano()
		peerId                = peer.ID()
		enodeUrl              = peer.Peer.Node().URLv4()
		newBlockAnnouncements = blockAnnouncesFromHashesAndNumbers(hashes, numbers)
	)

	if isAnnouncement {
		timestamp += p.announcePenalty
	}

	p.lock()
	defer p.unlock()

	for _, blockAnnouncement := range newBlockAnnouncements {
		if dist := int64(blockAnnouncement.number) - int64(p.handler.chain.CurrentBlock().NumberU64()); dist < -maxBlockDist {
			log.Warn("peri already seen this block so skip this new block announcement", "block", blockAnnouncement.number)
			continue
		}

		arrivalTimestamp, arrived := p.blockArrivals[blockAnnouncement]
		if arrived {
			// already seen this block then check which one is earlier
			if timestamp < arrivalTimestamp {
				p.blockArrivals[blockAnnouncement] = timestamp
			}
			p.blockArrivalPerPeer[blockAnnouncement][peerId] = timestamp
		} else {
			// first received then update information
			p.blockArrivals[blockAnnouncement] = timestamp
			p.blockArrivalPerPeer[blockAnnouncement] = map[string]int64{peerId: timestamp}
		}

		if p.config.PeriShowTxDelivery {
			if isAnnouncement {
				log.Info("receive block announcement", "peer", enodeUrl[enodeSplitIndex:], "blocknumber", blockAnnouncement.number)
			} else {
				log.Info("receive full block body", "peer", enodeUrl[enodeSplitIndex:], "blocknumber", blockAnnouncement.number)
			}
		}
	}
}

func (p *Peri) recordBlockBody(peer *eth.Peer, block *types.Block) {
	p.recordBlockAnnounces(peer, []common.Hash{block.Hash()}, []uint64{block.Number().Uint64()}, false)
}

func (p *Peri) recordTransactionAnnounces(peer *eth.Peer, hashes []common.Hash, isAnnouncement bool) {
	var (
		timestamp = time.Now().UnixNano()
		peerId    = peer.ID()
		enodeUrl  = peer.Peer.Node().URLv4()
	)

	if isAnnouncement {
		timestamp += p.announcePenalty
	}

	p.lock()
	defer p.unlock()

	for _, txHash := range hashes {
		if _, stale := p.txOldArrivals[txHash]; stale {
			// already seen this transaction so skip this new transaction announcement
			log.Warn("peri already seen this transaction so skip this new transaction announcement", "tx", txHash)
			continue
		}

		arrivalTimestamp, arrived := p.txArrivals[txHash]
		if arrived {
			// already seen this transaction then check which one is earlier
			if timestamp < arrivalTimestamp {
				p.txArrivals[txHash] = timestamp
			}
			p.txArrivalPerPeer[txHash][peerId] = timestamp
		} else {
			// first received then update information
			p.txArrivals[txHash] = timestamp
			p.txArrivalPerPeer[txHash] = map[string]int64{peerId: timestamp}
		}

		if p.config.PeriShowTxDelivery {
			log.Info("receive transaction announcement", "peer", enodeUrl[enodeSplitIndex:], "tx", fmt.Sprint(txHash))
		}
	}
}

func (p *Peri) recordTransactionBody(peer *eth.Peer, transactions []*types.Transaction) {
	var hashs = make([]common.Hash, 0, len(transactions))
	for _, tx := range transactions {
		hashs = append(hashs, tx.Hash())
	}
	p.recordTransactionAnnounces(peer, hashs, false)
}

func (p *Peri) getScores() ([]idScore, map[string]bool) {
	var (
		scores  []idScore
		excused = make(map[string]bool)

		latestArrivalTimestamp int64
		peerBirthTimestamp     int64
		peerDelayDuration      int64
		totalDelayDuration     int64
		peerForwardCount       int
		peerAverageDelay       float64
	)

	if p.approachingMiners == true {
		for _, arrivalTimestamp := range p.blockArrivals {
			if arrivalTimestamp > latestArrivalTimestamp {
				latestArrivalTimestamp = arrivalTimestamp
			}
		}

		peerForwardCount, totalDelayDuration, peerAverageDelay = 0, 0, 0.0
		for id, peer := range p.handler.peers.peers {
			p.peersSnapShot[id] = peer.Node().URLv4()
			peerBirthTimestamp = peer.Peer.ConnectedTimestamp
			for blockAnnouncement, arrivalTimestamp := range p.blockArrivals {
				if arrivalTimestamp < peerBirthTimestamp {
					continue
				}

				arrivalTimestampThisPeer, forwardThisPeer := p.blockArrivalPerPeer[blockAnnouncement][id]
				peerDelayDuration = arrivalTimestampThisPeer - arrivalTimestamp
				if forwardThisPeer == false || peerDelayDuration > p.maxDelayDuration {
					peerDelayDuration = p.maxDelayDuration
				}

				peerForwardCount += 1
				totalDelayDuration += peerDelayDuration
			}
			if peerForwardCount == 0 {
				// the peer maybe connect too late, if so, excuse it from computing scores temporarily
				if peerBirthTimestamp > latestArrivalTimestamp-p.config.PeriMaxDeliveryTolerance*milli2Nano {
					excused[id] = true
				}
				peerAverageDelay = float64(p.maxDelayDuration)
			} else {
				peerAverageDelay = float64(totalDelayDuration) / float64(peerForwardCount)
			}

			scores = append(scores, idScore{
				id:    id,
				score: peerAverageDelay,
			})
		}
	} else {
		for _, arrivalTimestamp := range p.txArrivals {
			/*
				// check whether transaction is target transaction
				// currently using a array indicates where it belongs to some sender
				if p.config.PeriTargeted {
					if _, isTarget := targetTx[txHash]; !isTarget {
						continue
					}
				}
			*/
			if arrivalTimestamp > latestArrivalTimestamp {
				latestArrivalTimestamp = arrivalTimestamp
			}
		}

		// loop through the current peers instead of recorded ones
		peerForwardCount, totalDelayDuration, peerAverageDelay = 0, 0, 0.0
		for id, peer := range p.handler.peers.peers {
			p.peersSnapShot[id] = peer.Node().URLv4()
			peerBirthTimestamp = peer.Peer.ConnectedTimestamp

			for tx, arrivalTimestamp := range p.txArrivals {
				/*
					if p.config.PeriTargeted {
						if _, isTarget := targetTx[tx]; !isTarget {
							continue
						}
					}
				*/
				if arrivalTimestamp < peerBirthTimestamp {
					continue
				}

				arrivalTimestampThisPeer, forwardThisPeer := p.txArrivalPerPeer[tx][id]
				peerDelayDuration = arrivalTimestampThisPeer - arrivalTimestamp
				if !forwardThisPeer || peerDelayDuration > p.maxDelayDuration {
					peerDelayDuration = p.maxDelayDuration
				}
				/*
					else if p.config.PeriTargeted {
						if !loggy.Config.FlagAllTx {
							if delay == 0 {
								loggy.ObserveAll(tx, peer.Node().URLv4(), firstArrival)
							}
						} else {
							loggy.ObserveAll(tx, peer.Node().URLv4(), arrival)
						}
					}
				*/
				peerForwardCount += 1
				totalDelayDuration += peerDelayDuration
			}

			if peerForwardCount == 0 {
				// the peer maybe connect too late, if so, excuse it from computing scores temporarily
				if peerBirthTimestamp > latestArrivalTimestamp-p.config.PeriMaxDeliveryTolerance*milli2Nano {
					excused[id] = true
				}
				peerAverageDelay = float64(p.maxDelayDuration)
			} else {
				peerAverageDelay = float64(totalDelayDuration) / float64(peerForwardCount)
			}

			scores = append(scores, idScore{id, peerAverageDelay})
		}
	}

	// Scores are sorted by descending order
	sort.Slice(scores, func(i, j int) bool {
		ndi, ndj := p.isNoDropPeer(scores[i].id), p.isNoDropPeer(scores[j].id)
		if ndi && !ndj {
			return false // give i lower priority when i cannot be dropped
		} else if ndj && !ndi {
			return true
		} else {
			return scores[i].score > scores[j].score
		}
	})

	return scores, excused
}

// check if a node is always undroppable (for instance, a predefined no drop ip list)
func (p *Peri) isNoDropPeer(id string) bool {
	var enode = p.peersSnapShot[id]
	var ipAddress = extractIPFromEnode(enode)

	for _, ip := range p.config.PeriNoDropList {
		if ip == ipAddress {
			return true
		}
	}
	return false
}

func (p *Peri) resetRecords() {
	// lock is assume to be held
	for tx, arrival := range p.txArrivals {
		p.txOldArrivals[tx] = arrival
	}

	// clear old arrival states which are assumed not to be forwarded anymore
	if len(p.txOldArrivals) > p.config.PeriMaxTransactionAmount {
		listArrivals := make([]struct {
			txHash           common.Hash
			arrivalTimestamp int64
		}, 0, len(p.txOldArrivals))

		for tx, arrival := range p.txOldArrivals {
			listArrivals = append(listArrivals, struct {
				txHash           common.Hash
				arrivalTimestamp int64
			}{tx, arrival})
		}

		// Sort arrival time by ascending order
		sort.Slice(listArrivals, func(i, j int) bool {
			return listArrivals[i].arrivalTimestamp < listArrivals[j].arrivalTimestamp
		})

		// Delete the earliest arrivals
		for i := 0; i < transactionArrivalReplace; i++ {
			delete(p.txOldArrivals, listArrivals[i].txHash)
		}
	}

	// reset arrival states
	p.txArrivals = make(map[common.Hash]int64)
	p.txArrivalPerPeer = make(map[common.Hash]map[string]int64)
	p.blockArrivals = make(map[blockAnnounce]int64)
	p.blockArrivalPerPeer = make(map[blockAnnounce]map[string]int64)
	p.peersSnapShot = make(map[string]string)
}

func (p *Peri) disconnectByScore() {
	p.locker.Lock()
	defer p.locker.Unlock()

	scores, excused := p.getScores()

	// number of peers to drop
	numDrop := p.replaceCount + len(scores) - p.handler.maxPeers
	if numDrop < 0 {
		numDrop = 0
	}

	// show logs on console and persistent some information
	p.summaryStats(scores, excused, numDrop)

	// Check if there is no block or transaction recorded. If so, everyone is excused and skip dropping peers
	if p.approachingMiners && len(p.blockArrivals) == 0 {
		log.Warn("no block recorded, peri policy skipped.", "peer count", p.handler.peers.len())
		return
	}
	if p.approachingMiners == false && len(p.txArrivals) == 0 {
		log.Warn("no transaction recorded, peri policy skipped.", "peer count", p.handler.peers.len())
		return
	}

	log.Info("before dropping during disconnect by score", "count", p.handler.peers.len())

	if !p.config.PeriActive { // Peri is inactive, drop randomly instead; Set ReplaceRatio=0 to disable dropping
		indices := make([]int, len(scores))
		for i := 0; i < len(indices); i++ {
			indices[i] = i
		}
		rand.Shuffle(len(indices), func(i, j int) {
			indices[i], indices[j] = indices[j], indices[i]
		})
		for i := 0; i < numDrop; i++ {
			id := scores[indices[i]].id
			p.handler.removePeer(id)
			p.handler.unregisterPeer(id)
		}
	} else { // Drop nodes, and add them to the blacklist
		for i := 0; i < numDrop; i++ {
			id := scores[i].id
			if _, isExcused := excused[id]; isExcused {
				continue
			}
			p.blacklist[extractIPFromEnode(p.peersSnapShot[id])] = true
			p.handler.removePeer(id)
			p.handler.unregisterPeer(id)
		}
	}

	log.Info("after dropping during disconnect by score", "count", p.handler.peers.len())
	p.resetRecords()
}

func extractIPFromEnode(enode string) string {
	parts := strings.Split(enode[enodeSplitIndex:], ":")
	return parts[0]
}

func (p *Peri) summaryStats(scores []idScore, excused map[string]bool, numDrop int) {
	timestamp := time.Now()
	log.Warn("peri policy is triggered", "timestamp", timestamp)
	p.fileLogger.Warn("peri policy is triggered", "timestamp", timestamp)
	blockCount, transactionCount, peerCount := len(p.blockArrivals), len(p.txArrivals), len(scores)
	if p.approachingMiners == true {
		log.Warn("Peri policy summary", "count of blocks", blockCount, "count of peers", peerCount, "count of drop", numDrop)
		p.fileLogger.Warn("Peri policy summary", "count of blocks", blockCount, "count of peers", peerCount, "count of drop", numDrop)
		if blockCount == 0 {
			return
		}
	} else {
		log.Warn("Peri policy summary", "count of transactions", transactionCount, "count of peers", peerCount, "count of drop", numDrop)
		p.fileLogger.Warn("Peri policy summary", "count of transactions", transactionCount, "count of peers", peerCount, "count of drop", numDrop)
		if transactionCount == 0 {
			return
		}
	}
	for _, element := range scores {
		log.Warn("Peri computation score of peers", "enode", p.peersSnapShot[element.id], "score", element.score)
		p.fileLogger.Warn("Peri computation score of peers", "enode", p.peersSnapShot[element.id], "score", element.score)
	}
}

func (p *Peri) isBlocked(enode string) bool {
	p.lock()
	defer p.unlock()
	_, blocked := p.blacklist[extractIPFromEnode(enode)]
	return blocked
}

func (p *Peri) broadcastBlockToPioplatPeer(peer *eth.Peer, block *types.Block, td *big.Int) {
	if dist := int64(block.NumberU64()) - int64(p.handler.chain.CurrentBlock().NumberU64()); dist < -maxBlockDist || dist > maxBlockDist {
		return
	}

	if p.handler.periBroadcast {
		// use map p.handler.periPeersIp to decide whether broadcast this block
		pioplatCount := 0
		p.handler.peers.lock.RLock()
		for _, ethPeerElement := range p.handler.peers.peers {
			peerIp := ethPeerElement.Node().IP().String()
			if _, found := p.handler.periPeersIp[peerIp]; found {
				if ethPeerElement.KnownBlock(block.Hash()) == false {
					ethPeerElement.AsyncSendNewBlock(block, td)
					log.Info("deliver block to pioplat peer", "block", block.NumberU64(), "from", peer.Node().IP().String(), "to", peerIp)
				}

				// all Pioplat nodes have been searched, ending early.
				pioplatCount += 1
				if pioplatCount >= len(p.handler.periPeersIp) {
					break
				}
			}
		}
		p.handler.peers.lock.RUnlock()
	}
}

func (p *Peri) broadcastTransactionsToPioplatPeer(txs []*types.Transaction) {
	if p.handler.periBroadcast {
		for _, tx := range txs {
			// use map p.handler.periPeersIp to decide whether broadcast this block
			pioplatCount := 0
			p.handler.peers.lock.RLock()
			for _, ethPeerElement := range p.handler.peers.peers {
				peerIp := ethPeerElement.Node().IP().String()
				if _, found := p.handler.periPeersIp[peerIp]; found {
					if ethPeerElement.KnownTransaction(tx.Hash()) == false {
						ethPeerElement.AsyncSendPooledTransactionHashes([]common.Hash{tx.Hash()})
						if p.config.PeriShowTxDelivery {
							log.Info("deliver transaction to pioplat peer", "tx", tx.Hash(), "ip", peerIp)
						}
					}

					// all Pioplat nodes have been searched, ending early.
					pioplatCount += 1
					if pioplatCount >= len(p.handler.periPeersIp) {
						break
					}
				}
			}
			p.handler.peers.lock.RUnlock()
		}
	}
}
