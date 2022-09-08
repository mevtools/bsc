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
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// PERI_AND_LATENCY_RECORDER_CODE_PIECE

const (
	Milli2Nano                = 1000000
	TransactionArrivalReplace = 30000
	BlockArrivalReplace       = 50
	EnodeSplitIndex           = 137
	PeriNodeDbPath            = "peri_nodes"
	PeriNodeCountKey          = "nodes_count"
	PeriNodeKeyPrefix         = "n_"
)

type Peri struct {
	config               *ethconfig.Config // ethconfig used globally during program execution
	handler              *handler          // implement handler to blocks and transactions arriving
	replaceCount         int               // count of replacement during every period
	maxDelayDuration     int64             // max delay duration in nano time
	blockAnnouncePenalty int64

	approachingMiners bool

	locker           *sync.Mutex                      // locker to protect the below map fields.
	txArrivals       map[common.Hash]int64            // record whether transactions are received, announcement and body are treated equally
	txArrivalPerPeer map[common.Hash]map[string]int64 // record transactions timestamp by peers
	txOldArrivals    map[common.Hash]int64            // record all stale transactions, todo: how to define it?

	blockArrivals       map[blockAnnounce]int64
	blockArrivalPerPeer map[blockAnnounce]map[string]int64
	blockOldArrivals    map[blockAnnounce]int64

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
		config:               config,
		handler:              h,
		locker:               new(sync.Mutex),
		replaceCount:         int(math.Round(float64(h.maxPeers) * config.PeriReplaceRatio)),
		maxDelayDuration:     int64(config.PeriMaxDelayPenalty * Milli2Nano),
		blockAnnouncePenalty: int64(config.PeriBlockAnnouncePenalty * Milli2Nano),
		approachingMiners:    true,
		txArrivals:           make(map[common.Hash]int64),
		txArrivalPerPeer:     make(map[common.Hash]map[string]int64),
		txOldArrivals:        make(map[common.Hash]int64),

		blockArrivals:       make(map[blockAnnounce]int64),
		blockArrivalPerPeer: make(map[blockAnnounce]map[string]int64),
		blockOldArrivals:    make(map[blockAnnounce]int64),

		peersSnapShot: make(map[string]string),
		blacklist:     make(map[string]bool),
		fileLogger:    log.New(),
	}

	databasePath := filepath.Join(config.PeriDataDirectory, PeriNodeDbPath)
	peri.nodesDb, err = enode.OpenDB(databasePath)
	if err != nil {
		log.Crit("open peri database failed", "err", err)
	}

	periNodesCount := peri.nodesDb.FetchUint64([]byte(PeriNodeCountKey))
	if periNodesCount > 0 {
		for i := 0; i < int(periNodesCount); i++ {
			enodeUrl := peri.nodesDb.FetchString([]byte(PeriNodeKeyPrefix + strconv.Itoa(i)))
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
			err = p.nodesDb.StoreInt64([]byte(PeriNodeCountKey), int64(len(scores)))
			if err != nil {
				log.Warn("peri store node count failed when exit", "err", err)
			}
			for i, element := range scores {
				enode := p.peersSnapShot[element.id]
				err = p.nodesDb.StoreString([]byte(PeriNodeKeyPrefix+strconv.Itoa(i)), enode)
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
		enode                 = peer.Peer.Node().URLv4()
		newBlockAnnouncements = blockAnnouncesFromHashesAndNumbers(hashes, numbers)
	)

	if isAnnouncement {
		timestamp += p.blockAnnouncePenalty
	}

	p.lock()
	defer p.unlock()

	for _, blockAnnouncement := range newBlockAnnouncements {
		if _, stale := p.blockOldArrivals[blockAnnouncement]; stale {
			// already seen this block so skip this new block announcement
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
				log.Info("receive block announcement", "peer", enode[EnodeSplitIndex:], "blocknumber", blockAnnouncement.number)
			} else {
				log.Info("receive full block body", "peer", enode[EnodeSplitIndex:], "blocknumber", blockAnnouncement.number)
			}
		}
	}

	// todo
}

func (p *Peri) recordBlockBody(peer *eth.Peer, block *types.Block) {
	p.recordBlockAnnounces(peer, []common.Hash{block.Hash()}, []uint64{block.Number().Uint64()}, false)
}

func (p *Peri) recordTransactionAnnounces(peer *eth.Peer, hashes []common.Hash) {
	var (
		timestamp = time.Now().UnixNano()
		peerId    = peer.ID()
		enode     = peer.Peer.Node().URLv4()
	)

	p.lock()
	defer p.unlock()

	for _, txAnnouncement := range hashes {
		if _, stale := p.txOldArrivals[txAnnouncement]; stale {
			// already seen this block so skip this new block announcement
			continue
		}

		arrivalTimestamp, arrived := p.txArrivals[txAnnouncement]
		if arrived {
			// already seen this block then check which one is earlier
			if timestamp < arrivalTimestamp {
				p.txArrivals[txAnnouncement] = timestamp
			}
			p.txArrivalPerPeer[txAnnouncement][peerId] = timestamp
		} else {
			// first received then update information
			p.txArrivals[txAnnouncement] = timestamp
			p.txArrivalPerPeer[txAnnouncement] = map[string]int64{peerId: timestamp}
		}

		if p.config.PeriShowTxDelivery {
			log.Info("receive transaction announcement", "peer", enode[EnodeSplitIndex:], "tx", fmt.Sprint(txAnnouncement))
		}
	}

	// todo
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
				if peerBirthTimestamp > latestArrivalTimestamp-p.config.PeriMaxDeliveryTolerance*Milli2Nano {
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

		/*
			for txHash, arrivalTimestamp := range p.txArrivals {
				if p.config.PeriTargeted {
					// check whether transaction is target transaction
					// currently using a array indicates where it belongs to some sender
					if _, isTarget := targetTx[txHash]; !isTarget {
						continue
					}
				}
				if arrivalTimestamp > latestArrivalTimestamp {
					latestArrivalTimestamp = arrivalTimestamp
				}
			}

			// loop through the current peers instead of recorded ones
			for id, peer := range p.handler.peers.peers {
				snapshot[id] = p.handler.peers.peers[id].Node().URLv4()
				birth := peer.Peer.Loggy_connectionStartTime.UnixNano()

				ntx, totalDelay, avgDelay := 0, int64(0), 0.0
				for tx, firstArrival := range arrivals {
					if p.config.PeriTargeted {
						if _, isTarget := targetTx[tx]; !isTarget {
							continue
						}
					}
					if firstArrival < birth {
						continue
					}

					arrival, forwarded := p.txArrivalPerPeer[tx][id]
					delay := arrival - firstArrival
					if !forwarded || delay > int64(p.config.PeriMaxDelayPenalty*Milli2Nano) {
						delay = int64(p.config.PeriMaxDelayPenalty * Milli2Nano)
					} else if p.config.PeriTargeted {
						if !loggy.Config.FlagAllTx {
							if delay == 0 {
								loggy.ObserveAll(tx, peer.Node().URLv4(), firstArrival)
							}
						} else {
							loggy.ObserveAll(tx, peer.Node().URLv4(), arrival)
						}
					}

					ntx++
					totalDelay += delay
				}

				if ntx == 0 { // Check if the peer is connected too late (if so, excuse it temporarily)
					avgDelay = float64(int64(p.config.PeriMaxDelayPenalty * Milli2Nano))
					if birth > latestArrival-p.config.PeriMaxDeliveryTolerance*Milli2Nano {
						excused[id] = true
					}
				} else {
					avgDelay = float64(totalDelay) / float64(ntx)
				}

				scores = append(scores, idScore{id, avgDelay})
			}
		*/
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
	for block, arrival := range p.blockArrivals {
		p.blockOldArrivals[block] = arrival
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
		for i := 0; i < TransactionArrivalReplace; i++ {
			delete(p.txOldArrivals, listArrivals[i].txHash)
		}
	}

	if len(p.blockOldArrivals) > p.config.PeriMaxBlockAmount {
		listArrivals := make([]struct {
			blockAnnouncement blockAnnounce
			arrivalTimestamp  int64
		}, 0, len(p.blockOldArrivals))

		for block, arrival := range p.blockOldArrivals {
			listArrivals = append(listArrivals, struct {
				blockAnnouncement blockAnnounce
				arrivalTimestamp  int64
			}{block, arrival})
		}

		sort.Slice(listArrivals, func(i, j int) bool {
			return listArrivals[i].arrivalTimestamp < listArrivals[j].arrivalTimestamp
		})

		for i := 0; i < BlockArrivalReplace; i++ {
			delete(p.blockOldArrivals, listArrivals[i].blockAnnouncement)
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

	// Check if there is no tx recorded. If so, everyone is excused and skip dropping peers
	if len(p.blockArrivals) == 0 {
		log.Warn("no block recorded, peri policy skipped.", "peer count", p.handler.peers.len())
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
	//parts := strings.Split(enode, "@")
	//parts = strings.Split(parts[len(parts)-1], ":")
	//return parts[0]
	return enode[EnodeSplitIndex:]
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
		for _, element := range scores {
			log.Warn("Peri computation score of peers", "enode", p.peersSnapShot[element.id], "score", element.score)
			p.fileLogger.Warn("Peri computation score of peers", "enode", p.peersSnapShot[element.id], "score", element.score)
		}
		for blockAnnouncement := range p.blockArrivalPerPeer {
			p.fileLogger.Warn("Peri record per peer sent blocks", "block", blockAnnouncement.number, "value", p.blockArrivalPerPeer[blockAnnouncement])
		}
	} else {
		log.Warn("Peri policy summary", "count of transactions", transactionCount, "count of peers", peerCount, "count of drop", numDrop)
		p.fileLogger.Warn("Peri policy summary", "count of transactions", transactionCount, "count of peers", peerCount, "count of drop", numDrop)
		if transactionCount == 0 {
			return
		}
	}
}

func (p *Peri) isBlocked(enode string) bool {
	_, blocked := p.blacklist[extractIPFromEnode(enode)]
	return blocked
}

func (p *Peri) broadcastBlockToPioplatPeer(block *types.Block) {

}
