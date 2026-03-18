package wechat

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/git-jiadong/go-lame"
	"github.com/git-jiadong/go-silk"
	_ "github.com/mattn/go-sqlite3"
	"github.com/shirou/gopsutil/v3/process"
	"golang.org/x/sys/windows"
)

type WeChatInfo struct {
	ProcessID    uint32
	KeyProcessID uint32  // Process ID containing WeChatWin.dll (for key extraction)
	FilePath     string
	AcountName   string
	Version      string
	Is64Bits     bool
	DllBaseAddr  uintptr
	DllBaseSize  uint32
	DBKey        string
}

type WeChatInfoList struct {
	Info  []WeChatInfo `json:"Info"`
	Total int          `json:"Total"`
}

type wechatMediaMSG struct {
	Key      string
	MsgSvrID int
	Buf      []byte
}

type wechatHeadImgMSG struct {
	userName string
	Buf      []byte
}

func GetWeChatAllInfo() *WeChatInfoList {
	list := GetWeChatInfo()

	for i := range list.Info {
		list.Info[i].DBKey = GetWeChatKey(&list.Info[i])
	}

	return list
}

func ExportWeChatAllData(info WeChatInfo, expPath string, progress chan<- string) {
	defer close(progress)
	fileInfo, err := os.Stat(info.FilePath)
	if err != nil || !fileInfo.IsDir() {
		progress <- fmt.Sprintf("{\"status\":\"error\", \"result\":\"%s error\"}", info.FilePath)
		return
	}
	if !exportWeChatDateBase(info, expPath, progress) {
		return
	}

	exportWeChatBat(info, expPath, progress)
	exportWeChatVideoAndFile(info, expPath, progress)
	exportWeChatVoice(info, expPath, progress)
	exportWeChatHeadImage(info, expPath, progress)
}

func exportWeChatHeadImage(info WeChatInfo, expPath string, progress chan<- string) {
	progress <- "{\"status\":\"processing\", \"result\":\"export WeChat Head Image\", \"progress\": 81}"

	headImgPath := fmt.Sprintf("%s\\FileStorage\\HeadImage", expPath)
	if _, err := os.Stat(headImgPath); err != nil {
		if err := os.MkdirAll(headImgPath, 0644); err != nil {
			log.Printf("MkdirAll %s failed: %v\n", headImgPath, err)
			progress <- fmt.Sprintf("{\"status\":\"error\", \"result\":\"%v error\"}", err)
			return
		}
	}

	handleNumber := int64(0)
	fileNumber := int64(0)

	var wg sync.WaitGroup
	var reportWg sync.WaitGroup
	quitChan := make(chan struct{})
	MSGChan := make(chan wechatHeadImgMSG, 100)
	go func() {
		for {
			miscDBPath := fmt.Sprintf("%s\\Msg\\Misc.db", expPath)
			_, err := os.Stat(miscDBPath)
			if err != nil {
				log.Println("no exist:", miscDBPath)
				break
			}

			db, err := sql.Open("sqlite3", miscDBPath)
			if err != nil {
				log.Printf("open %s failed: %v\n", miscDBPath, err)
				break
			}
			defer db.Close()

			err = db.QueryRow("select count(*) from ContactHeadImg1;").Scan(&fileNumber)
			if err != nil {
				log.Println("select count(*) failed", err)
				break
			}
			log.Println("ContactHeadImg1 fileNumber", fileNumber)
			rows, err := db.Query("select ifnull(usrName,'') as usrName, ifnull(smallHeadBuf,'') as smallHeadBuf from ContactHeadImg1;")
			if err != nil {
				log.Printf("Query failed: %v\n", err)
				break
			}

			msg := wechatHeadImgMSG{}
			for rows.Next() {
				err := rows.Scan(&msg.userName, &msg.Buf)
				if err != nil {
					log.Println("Scan failed: ", err)
					break
				}

				MSGChan <- msg
			}
			break
		}
		close(MSGChan)
	}()

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range MSGChan {
				imgPath := fmt.Sprintf("%s\\%s.headimg", headImgPath, msg.userName)
				for {
					// log.Println("imgPath:", imgPath, len(msg.Buf))
					_, err := os.Stat(imgPath)
					if err == nil {
						break
					}
					if len(msg.userName) == 0 || len(msg.Buf) == 0 {
						break
					}
					err = os.WriteFile(imgPath, msg.Buf[:], 0666)
					if err != nil {
						log.Println("WriteFile:", imgPath, err)
					}
					break
				}
				atomic.AddInt64(&handleNumber, 1)
			}
		}()
	}

	reportWg.Add(1)
	go func() {
		defer reportWg.Done()
		for {
			select {
			case <-quitChan:
				log.Println("WeChat Head Image report progress end")
				return
			default:
				if fileNumber != 0 {
					filePercent := float64(handleNumber) / float64(fileNumber)
					totalPercent := 81 + (filePercent * (100 - 81))
					totalPercentStr := fmt.Sprintf("{\"status\":\"processing\", \"result\":\"export WeChat Head Image doing\", \"progress\": %d}", int(totalPercent))
					progress <- totalPercentStr
				}
				time.Sleep(time.Second)
			}
		}
	}()

	wg.Wait()
	close(quitChan)
	reportWg.Wait()
	progress <- "{\"status\":\"processing\", \"result\":\"export WeChat Head Image end\", \"progress\": 100}"
}

func exportWeChatVoice(info WeChatInfo, expPath string, progress chan<- string) {
	progress <- "{\"status\":\"processing\", \"result\":\"export WeChat voice start\", \"progress\": 61}"

	voicePath := fmt.Sprintf("%s\\FileStorage\\Voice", expPath)
	if _, err := os.Stat(voicePath); err != nil {
		if err := os.MkdirAll(voicePath, 0644); err != nil {
			log.Printf("MkdirAll %s failed: %v\n", voicePath, err)
			progress <- fmt.Sprintf("{\"status\":\"error\", \"result\":\"%v error\"}", err)
			return
		}
	}

	handleNumber := int64(0)
	fileNumber := int64(0)
	index := 0
	for {
		mediaMSGDB := fmt.Sprintf("%s\\Msg\\Multi\\MediaMSG%d.db", expPath, index)
		_, err := os.Stat(mediaMSGDB)
		if err != nil {
			break
		}
		index += 1
		fileNumber += 1
	}

	var wg sync.WaitGroup
	var reportWg sync.WaitGroup
	quitChan := make(chan struct{})
	index = -1
	MSGChan := make(chan wechatMediaMSG, 100)
	go func() {
		for {
			index += 1
			mediaMSGDB := fmt.Sprintf("%s\\Msg\\Multi\\MediaMSG%d.db", expPath, index)
			_, err := os.Stat(mediaMSGDB)
			if err != nil {
				break
			}

			db, err := sql.Open("sqlite3", mediaMSGDB)
			if err != nil {
				log.Printf("open %s failed: %v\n", mediaMSGDB, err)
				continue
			}
			defer db.Close()

			rows, err := db.Query("select Key, Reserved0, Buf from Media;")
			if err != nil {
				log.Printf("Query failed: %v\n", err)
				continue
			}

			msg := wechatMediaMSG{}
			for rows.Next() {
				err := rows.Scan(&msg.Key, &msg.MsgSvrID, &msg.Buf)
				if err != nil {
					log.Println("Scan failed: ", err)
					break
				}

				MSGChan <- msg
			}
			atomic.AddInt64(&handleNumber, 1)
		}
		close(MSGChan)
	}()

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range MSGChan {
				mp3Path := fmt.Sprintf("%s\\%d.mp3", voicePath, msg.MsgSvrID)
				_, err := os.Stat(mp3Path)
				if err == nil {
					continue
				}

				err = silkToMp3(msg.Buf[:], mp3Path)
				if err != nil {
					log.Printf("silkToMp3 %s failed: %v\n", mp3Path, err)
				}
			}
		}()
	}

	reportWg.Add(1)
	go func() {
		defer reportWg.Done()
		for {
			select {
			case <-quitChan:
				log.Println("WeChat voice report progress end")
				return
			default:
				filePercent := float64(handleNumber) / float64(fileNumber)
				totalPercent := 61 + (filePercent * (80 - 61))
				totalPercentStr := fmt.Sprintf("{\"status\":\"processing\", \"result\":\"export WeChat voice doing\", \"progress\": %d}", int(totalPercent))
				progress <- totalPercentStr
				time.Sleep(time.Second)
			}
		}
	}()

	wg.Wait()
	close(quitChan)
	reportWg.Wait()
	progress <- "{\"status\":\"processing\", \"result\":\"export WeChat voice end\", \"progress\": 80}"
}

func exportWeChatVideoAndFile(info WeChatInfo, expPath string, progress chan<- string) {
	progress <- "{\"status\":\"processing\", \"result\":\"export WeChat Video and File start\", , \"progress\": 41}"
	videoRootPath := info.FilePath + "\\FileStorage\\Video"
	fileRootPath := info.FilePath + "\\FileStorage\\File"
	cacheRootPath := info.FilePath + "\\FileStorage\\Cache"

	rootPaths := []string{videoRootPath, fileRootPath, cacheRootPath}

	handleNumber := int64(0)
	fileNumber := int64(0)
	for _, path := range rootPaths {
		fileNumber += getPathFileNumber(path, "")
	}
	log.Println("VideoAndFile ", fileNumber)

	var wg sync.WaitGroup
	var reportWg sync.WaitGroup
	quitChan := make(chan struct{})
	taskChan := make(chan [2]string, 100)
	go func() {
		for _, rootPath := range rootPaths {
			log.Println(rootPath)
			if _, err := os.Stat(rootPath); err != nil {
				continue
			}
			err := filepath.Walk(rootPath, func(path string, finfo os.FileInfo, err error) error {
				if err != nil {
					log.Printf("filepath.Walk：%v\n", err)
					return err
				}

				if !finfo.IsDir() {
					expFile := expPath + path[len(info.FilePath):]
					_, err := os.Stat(filepath.Dir(expFile))
					if err != nil {
						os.MkdirAll(filepath.Dir(expFile), 0644)
					}

					task := [2]string{path, expFile}
					taskChan <- task
					return nil
				}

				return nil
			})
			if err != nil {
				log.Println("filepath.Walk:", err)
				progress <- fmt.Sprintf("{\"status\":\"error\", \"result\":\"%v\"}", err)
			}
		}
		close(taskChan)
	}()

	for i := 1; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskChan {
				_, err := os.Stat(task[1])
				if err == nil {
					atomic.AddInt64(&handleNumber, 1)
					continue
				}
				_, err = copyFile(task[0], task[1])
				if err != nil {
					log.Println("DecryptDat:", err)
					progress <- fmt.Sprintf("{\"status\":\"error\", \"result\":\"copyFile %v\"}", err)
				}
				atomic.AddInt64(&handleNumber, 1)
			}
		}()
	}
	reportWg.Add(1)
	go func() {
		defer reportWg.Done()
		for {
			select {
			case <-quitChan:
				log.Println("WeChat Video and File report progress end")
				return
			default:
				filePercent := float64(handleNumber) / float64(fileNumber)
				totalPercent := 41 + (filePercent * (60 - 41))
				totalPercentStr := fmt.Sprintf("{\"status\":\"processing\", \"result\":\"export WeChat Video and File doing\", \"progress\": %d}", int(totalPercent))
				progress <- totalPercentStr
				time.Sleep(time.Second)
			}
		}
	}()
	wg.Wait()
	close(quitChan)
	reportWg.Wait()
	progress <- "{\"status\":\"processing\", \"result\":\"export WeChat Video and File end\", \"progress\": 60}"
}

func exportWeChatBat(info WeChatInfo, expPath string, progress chan<- string) {
	progress <- "{\"status\":\"processing\", \"result\":\"export WeChat Dat start\", \"progress\": 21}"
	datRootPath := info.FilePath + "\\FileStorage\\MsgAttach"
	imageRootPath := info.FilePath + "\\FileStorage\\Image"
	rootPaths := []string{datRootPath, imageRootPath}

	handleNumber := int64(0)
	fileNumber := int64(0)
	for i := range rootPaths {
		fileNumber += getPathFileNumber(rootPaths[i], ".dat")
	}
	log.Println("DatFileNumber ", fileNumber)

	var wg sync.WaitGroup
	var reportWg sync.WaitGroup
	quitChan := make(chan struct{})
	taskChan := make(chan [2]string, 100)
	go func() {
		for i := range rootPaths {
			if _, err := os.Stat(rootPaths[i]); err != nil {
				continue
			}

			err := filepath.Walk(rootPaths[i], func(path string, finfo os.FileInfo, err error) error {
				if err != nil {
					log.Printf("filepath.Walk：%v\n", err)
					return err
				}

				if !finfo.IsDir() && strings.HasSuffix(path, ".dat") {
					expFile := expPath + path[len(info.FilePath):]
					_, err := os.Stat(filepath.Dir(expFile))
					if err != nil {
						os.MkdirAll(filepath.Dir(expFile), 0644)
					}

					task := [2]string{path, expFile}
					taskChan <- task
					return nil
				}

				return nil
			})

			if err != nil {
				log.Println("filepath.Walk:", err)
				progress <- fmt.Sprintf("{\"status\":\"error\", \"result\":\"%v\"}", err)
			}
		}
		close(taskChan)
	}()

	for i := 1; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskChan {
				_, err := os.Stat(task[1])
				if err == nil {
					atomic.AddInt64(&handleNumber, 1)
					continue
				}
				err = DecryptDat(task[0], task[1])
				if err != nil {
					log.Println("DecryptDat:", err)
					progress <- fmt.Sprintf("{\"status\":\"error\", \"result\":\"DecryptDat %v\"}", err)
				}
				atomic.AddInt64(&handleNumber, 1)
			}
		}()
	}
	reportWg.Add(1)
	go func() {
		defer reportWg.Done()
		for {
			select {
			case <-quitChan:
				log.Println("WeChat Dat report progress end")
				return
			default:
				filePercent := float64(handleNumber) / float64(fileNumber)
				totalPercent := 21 + (filePercent * (40 - 21))
				totalPercentStr := fmt.Sprintf("{\"status\":\"processing\", \"result\":\"export WeChat Dat doing\", \"progress\": %d}", int(totalPercent))
				progress <- totalPercentStr
				time.Sleep(time.Second)
			}
		}
	}()
	wg.Wait()
	close(quitChan)
	reportWg.Wait()
	progress <- "{\"status\":\"processing\", \"result\":\"export WeChat Dat end\", \"progress\": 40}"
}

func exportWeChatDateBase(info WeChatInfo, expPath string, progress chan<- string) bool {

	progress <- "{\"status\":\"processing\", \"result\":\"export WeChat DateBase start\", \"progress\": 1}"

	dbKey, err := hex.DecodeString(info.DBKey)
	if err != nil {
		log.Println("DecodeString:", err)
		progress <- fmt.Sprintf("{\"status\":\"error\", \"result\":\"%v\"}", err)
		return false
	}

	handleNumber := int64(0)
	fileNumber := getPathFileNumber(info.FilePath+"\\Msg", ".db")
	var wg sync.WaitGroup
	var reportWg sync.WaitGroup
	quitChan := make(chan struct{})
	taskChan := make(chan [2]string, 20)
	go func() {
		err = filepath.Walk(info.FilePath+"\\Msg", func(path string, finfo os.FileInfo, err error) error {
			if err != nil {
				log.Printf("filepath.Walk：%v\n", err)
				return err
			}
			if !finfo.IsDir() && strings.HasSuffix(path, ".db") {
				expFile := expPath + path[len(info.FilePath):]
				_, err := os.Stat(filepath.Dir(expFile))
				if err != nil {
					os.MkdirAll(filepath.Dir(expFile), 0644)
				}

				task := [2]string{path, expFile}
				taskChan <- task
			}

			return nil
		})
		if err != nil {
			log.Println("filepath.Walk:", err)
			progress <- fmt.Sprintf("{\"status\":\"error\", \"result\":\"%v\"}", err)
		}
		close(taskChan)
	}()

	for i := 1; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskChan {
				if filepath.Base(task[0]) == "xInfo.db" {
					copyFile(task[0], task[1])
				} else {
					err = DecryptDataBase(task[0], dbKey, task[1])
					if err != nil {
						log.Println("DecryptDataBase:", err)
						progress <- fmt.Sprintf("{\"status\":\"error\", \"result\":\"%s %v\"}", task[0], err)
					}
				}
				atomic.AddInt64(&handleNumber, 1)
			}
		}()
	}

	reportWg.Add(1)
	go func() {
		defer reportWg.Done()
		for {
			select {
			case <-quitChan:
				log.Println("WeChat DateBase report progress end")
				return
			default:
				filePercent := float64(handleNumber) / float64(fileNumber)
				totalPercent := 1 + (filePercent * (20 - 1))
				totalPercentStr := fmt.Sprintf("{\"status\":\"processing\", \"result\":\"export WeChat DateBase doing\", \"progress\": %d}", int(totalPercent))
				progress <- totalPercentStr
				time.Sleep(time.Second)
			}
		}
	}()
	wg.Wait()
	close(quitChan)
	reportWg.Wait()
	progress <- "{\"status\":\"processing\", \"result\":\"export WeChat DateBase end\", \"progress\": 20}"
	return true
}

func GetWeChatInfo() (list *WeChatInfoList) {
	list = &WeChatInfoList{}
	list.Info = make([]WeChatInfo, 0)
	list.Total = 0

	log.Println("Scanning for WeChat processes...")

	processes, err := process.Processes()
	if err != nil {
		log.Println("Error getting processes:", err)
		return
	}

	// Pass 1: Find all processes with open media databases (database processes)
	dbProcesses := make([]WeChatInfo, 0)
	for _, p := range processes {
		name, err := p.Name()
		if err != nil {
			continue
		}
		// Debug: log all WeChat-related processes found
		if strings.Contains(strings.ToLower(name), "weixin") || strings.Contains(strings.ToLower(name), "wechat") {
			log.Printf("Found WeChat-related process: %s (PID: %d)", name, p.Pid)
		}
		if name == "WeChat.exe" || name == "Weixin.exe" {
			info := WeChatInfo{}
			info.ProcessID = uint32(p.Pid)
			info.KeyProcessID = info.ProcessID // Default to same process
			info.Is64Bits, _ = Is64BitProcess(info.ProcessID)

			files, err := p.OpenFiles()
			if err != nil {
				log.Printf("OpenFiles failed for PID %d: %v", p.Pid, err)
				continue
			}

			for _, f := range files {
				// Support both old (Media.db) and new (media_0.db) WeChat database structures
				if strings.HasSuffix(f.Path, "\\Media.db") || strings.HasSuffix(f.Path, "\\media_0.db") {
					log.Printf("Found media database in PID %d: %s", p.Pid, f.Path)
					filePath := f.Path
					// Remove Windows long path prefix \\?\
					if strings.HasPrefix(filePath, "\\\\?\\") {
						filePath = filePath[4:]
					}
					parts := strings.Split(filePath, string(filepath.Separator))
					if len(parts) < 4 {
						log.Println("Error filePath " + filePath)
						break
					}

					// Detect if this is new or old WeChat structure based on path
					isNewStructure := false
					for i, part := range parts {
						if part == "db_storage" && i > 0 {
							isNewStructure = true
							// In new structure, wxid is the part before db_storage
							info.AcountName = parts[i-1]
							info.FilePath = strings.Join(parts[:i], string(filepath.Separator))
							log.Printf("Using new WeChat structure - AccountName: [%s], FilePath: [%s]", info.AcountName, info.FilePath)
							break
						}
					}

					// Fall back to old structure parsing
					if !isNewStructure {
						info.FilePath = strings.Join(parts[:len(parts)-2], string(filepath.Separator))
						info.AcountName = strings.Join(parts[len(parts)-3:len(parts)-2], string(filepath.Separator))
						log.Printf("Using old WeChat structure - AccountName: [%s], FilePath: [%s]", info.AcountName, info.FilePath)
					}
					dbProcesses = append(dbProcesses, info)
					break
				}
			}
		}
	}

	if len(dbProcesses) == 0 {
		log.Println("No database processes found")
		return
	}
	log.Printf("Found %d database process(es)", len(dbProcesses))

	// Pass 2: Find main process with WeChatWin.dll
	var mainProcess *WeChatInfo
	for _, p := range processes {
		name, err := p.Name()
		if err != nil {
			continue
		}
		if name == "WeChat.exe" || name == "Weixin.exe" {
			hModuleSnap, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPMODULE|windows.TH32CS_SNAPMODULE32, uint32(p.Pid))
			if err != nil {
				continue
			}
			defer windows.CloseHandle(hModuleSnap)

			var me32 windows.ModuleEntry32
			me32.Size = uint32(windows.SizeofModuleEntry32)

			err = windows.Module32First(hModuleSnap, &me32)
			if err != nil {
				continue
			}

			log.Printf("Searching for WeChatWin.dll/Weixin.dll in PID %d...", p.Pid)
			dllCount := 0
			wechatDlls := make([]string, 0)
			for ; err == nil; err = windows.Module32Next(hModuleSnap, &me32) {
				moduleName := windows.UTF16ToString(me32.Module[:])
				dllCount++
				// Log DLLs containing WeChat/Weixin/wx keywords
				if strings.Contains(strings.ToLower(moduleName), "wechat") ||
				   strings.Contains(strings.ToLower(moduleName), "weixin") ||
				   strings.Contains(strings.ToLower(moduleName), "wx") ||
				   strings.Contains(strings.ToLower(moduleName), "dll") {
					wechatDlls = append(wechatDlls, moduleName)
				}
				// Support both old (WeChatWin.dll) and new (Weixin.dll) DLL names
				if moduleName == "WeChatWin.dll" || moduleName == "Weixin.dll" {
					log.Printf("Found main process with %s: PID %d", moduleName, p.Pid)
					mainProcess = &WeChatInfo{}
					mainProcess.ProcessID = uint32(p.Pid)
					mainProcess.KeyProcessID = uint32(p.Pid)
					mainProcess.Is64Bits, _ = Is64BitProcess(mainProcess.ProcessID)
					mainProcess.DllBaseAddr = me32.ModBaseAddr
					mainProcess.DllBaseSize = me32.ModBaseSize

					// Get version info
					var zero windows.Handle
					driverPath := windows.UTF16ToString(me32.ExePath[:])
					infoSize, err := windows.GetFileVersionInfoSize(driverPath, &zero)
					if err == nil {
						versionInfo := make([]byte, infoSize)
						if err = windows.GetFileVersionInfo(driverPath, 0, infoSize, unsafe.Pointer(&versionInfo[0])); err == nil {
							var fixedInfo *windows.VS_FIXEDFILEINFO
							fixedInfoLen := uint32(unsafe.Sizeof(*fixedInfo))
							err = windows.VerQueryValue(unsafe.Pointer(&versionInfo[0]), `\`, (unsafe.Pointer)(&fixedInfo), &fixedInfoLen)
							if err == nil {
								mainProcess.Version = fmt.Sprintf("%d.%d.%d.%d",
									(fixedInfo.FileVersionMS>>16)&0xff,
									(fixedInfo.FileVersionMS>>0)&0xff,
									(fixedInfo.FileVersionLS>>16)&0xff,
									(fixedInfo.FileVersionLS>>0)&0xff)
								log.Printf("Main process version: %s", mainProcess.Version)
							}
						}
					}
					break
				}
			}
			if mainProcess == nil {
				// If WeChatWin.dll/Weixin.dll not found, log WeChat-related DLLs for debugging
				log.Printf("PID %d: Scanned %d DLLs", p.Pid, dllCount)
				if len(wechatDlls) > 0 {
					log.Printf("PID %d: WeChat-related DLLs: %v", p.Pid, wechatDlls)
				}
			}
			if mainProcess != nil {
				break // Found main process
			}
		}
	}

	if mainProcess == nil {
		log.Println("Warning: Could not find main process with WeChatWin.dll")
		// Try to use database processes as-is (may not work for key extraction)
		for _, info := range dbProcesses {
			list.Info = append(list.Info, info)
			list.Total++
		}
		log.Printf("GetWeChatInfo completed: found %d WeChat account(s) (without main process)", list.Total)
		return
	}

	// Pass 3: Merge - use main process's DLL info for all database processes
	for i := range dbProcesses {
		dbProcesses[i].KeyProcessID = mainProcess.ProcessID
		dbProcesses[i].DllBaseAddr = mainProcess.DllBaseAddr
		dbProcesses[i].DllBaseSize = mainProcess.DllBaseSize
		dbProcesses[i].Version = mainProcess.Version
		list.Info = append(list.Info, dbProcesses[i])
		list.Total++
		log.Printf("Merged: account [%s] from DB PID %d using DLL from main PID %d",
			dbProcesses[i].AcountName, dbProcesses[i].ProcessID, mainProcess.ProcessID)
	}

	log.Printf("GetWeChatInfo completed: found %d WeChat account(s)", list.Total)
	return
}

func Is64BitProcess(pid uint32) (bool, error) {
	is64Bit := false
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_INFORMATION, false, pid)
	if err != nil {
		log.Println("Error opening process:", err)
		return is64Bit, errors.New("OpenProcess failed")
	}
	defer windows.CloseHandle(handle)

	err = windows.IsWow64Process(handle, &is64Bit)
	if err != nil {
		log.Println("Error IsWow64Process:", err)
	}
	return !is64Bit, err
}

func GetWeChatKey(info *WeChatInfo) string {
	// 支持新版和旧版微信数据库路径
	var mediaDB string

	// 尝试新版路径
	newMediaPath := info.FilePath + "\\db_storage\\message\\media_0.db"
	if _, err := os.Stat(newMediaPath); err == nil {
		mediaDB = newMediaPath
		log.Println("Using new Media DB path:", mediaDB)
	} else {
		// 使用旧版路径
		mediaDB = info.FilePath + "\\Msg\\Media.db"
		if _, err := os.Stat(mediaDB); err != nil {
			log.Printf("open db %s error: %v", mediaDB, err)
			return ""
		}
		log.Println("Using old Media DB path:", mediaDB)
	}

	// Use KeyProcessID for memory reading (main process with WeChatWin.dll)
	keyProcessID := info.KeyProcessID
	if keyProcessID == 0 {
		keyProcessID = info.ProcessID // Fallback to database process
	}
	log.Printf("Opening process PID %d for key extraction (database process: %d)", keyProcessID, info.ProcessID)

	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_INFORMATION|windows.PROCESS_VM_READ, false, uint32(keyProcessID))
	if err != nil {
		log.Printf("Error opening process %d: %v", keyProcessID, err)
		return ""
	}
	defer windows.CloseHandle(handle)

	if info.DllBaseAddr == 0 || info.DllBaseSize == 0 {
		log.Printf("Invalid DLL info: BaseAddr=0x%X, BaseSize=%d", info.DllBaseAddr, info.DllBaseSize)
		return ""
	}

	log.Printf("Reading process memory: PID=%d, Addr=0x%X, Size=%d", keyProcessID, info.DllBaseAddr, info.DllBaseSize)
	buffer := make([]byte, info.DllBaseSize)
	err = windows.ReadProcessMemory(handle, uintptr(info.DllBaseAddr), &buffer[0], uintptr(len(buffer)), nil)
	if err != nil {
		log.Printf("Error ReadProcessMemory: %v", err)
		return ""
	}

	// Determine max search size (limit to avoid excessive memory scanning)
	// WeChat 4.0+ may cache keys in various locations, so we need to search more memory
	maxSearchSize := int(info.DllBaseSize)
	if maxSearchSize > 50*1024*1024 { // Increased limit to 50MB for better coverage
		maxSearchSize = 50 * 1024 * 1024
	}
	log.Printf("Starting key search in memory buffer (size: %d, max search: %d)", len(buffer), maxSearchSize)

	// === Method 1: WCDB Cached Key Pattern (NEW - works for PC WeChat 4.0+) ===
	log.Println("=== Method 1: Scanning for WCDB cached key pattern ===")
	key := findWCDBCachedKeys(buffer, mediaDB, maxSearchSize)
	if key != "" {
		log.Printf("✓ Method 1 SUCCESS: Found WCDB cached key!")
		return key
	}
	log.Println("× Method 1 FAILED: WCDB cached key pattern not found")

	// === Method 2: Device Symbol Search (OLD - for mobile WeChat) ===
	log.Println("=== Method 2: Trying device symbol search (legacy method) ===")
	offset := 0
	attemptCount := 0
	for {
		if offset >= maxSearchSize-100 {
			log.Printf("Search completed: reached max offset %d", offset)
			break
		}
		index := hasDeviceSybmol(buffer[offset:])
		if index == -1 {
			log.Println("has not DeviceSybmol - tried to find device identifiers (android, iphone, ipad, etc.)")
			log.Printf("Searched %d bytes without finding device symbols", offset)
			break
		}
		attemptCount++
		log.Printf("Attempt %d: hasDeviceSybmol found at offset 0x%X", attemptCount, index)
		fmt.Printf("hasDeviceSybmol: 0x%X\n", index)
		keys := findDBKeyPtr(buffer[offset:index], info.Is64Bits)
		log.Printf("Found %d potential key pointers", len(keys))

		key, err := findDBkey(handle, mediaDB, keys)
		if err == nil {
			log.Printf("✓ Method 2 SUCCESS: Successfully extracted database key!")
			return key
		} else {
			log.Printf("findDBkey failed with %d keys: %v", len(keys), err)
		}

		offset += (index + 20)
	}
	log.Printf("× Method 2 FAILED: Key extraction failed after %d attempts", attemptCount)

	return ""
}

// findWCDBCachedKeys scans memory for WCDB cached key pattern: x'<hex_chars>'
// WCDB (WeChat Database) caches derived keys in memory with this specific format
// This approach works for PC WeChat 4.0+ and doesn't rely on mobile device identifiers
//
// Reference implementation: ylytdeng/wechat-decrypt/key_scan_common.py
// Pattern: x'([0-9a-fA-F]{64,192})' - matches 64 to 192 hex characters
//
// The format can vary:
// - 96 hex chars (64 key + 32 salt) - standard WCDB format
// - 128 hex chars (64 key + 64 salt) - extended format
// - Variable lengths between 64-192 chars
func findWCDBCachedKeys(buffer []byte, mediaDB string, maxSearchSize int) string {
	log.Printf("Scanning for WCDB cached key pattern in %d bytes (max: %d)", len(buffer), maxSearchSize)

	// Debug: count how many "x" (0x78) bytes we find in the first 1MB
	xCount := 0
	for i := 0; i < 1024*1024 && i < len(buffer); i++ {
		if buffer[i] == 0x78 {
			xCount++
		}
	}
	log.Printf("Debug: Found %d 'x' (0x78) bytes in first 1MB", xCount)

	// Search size limited to avoid excessive scanning
	searchSize := len(buffer)
	if searchSize > maxSearchSize {
		searchSize = maxSearchSize
	}

	attemptCount := 0
	verifiedCount := 0

	// Helper function to check if a byte is a valid hex character
	isHexChar := func(b byte) bool {
		return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
	}

	// Helper function to extract hex string at position starting from hexStart
	// Returns the hex string and the position of the closing quote, or empty string if invalid
	extractHexString := func(buf []byte, start int, isUTF16 bool) ([]byte, int) {
		var hexStr []byte
		i := start

		if isUTF16 {
			// UTF-16LE: extract every other byte (skip the 0x00 bytes)
			for i+1 < len(buf) {
				if buf[i] == 0x27 && buf[i+1] == 0x00 {
					// Found closing quote "'" in UTF-16LE
					return hexStr, i + 2
				}
				if buf[i+1] != 0x00 {
					// Not valid UTF-16LE (high byte should be 0x00)
					return nil, -1
				}
				if !isHexChar(buf[i]) {
					// Non-hex character found
					return nil, -1
				}
				hexStr = append(hexStr, buf[i])
				i += 2

				// Safety limit to prevent infinite loops
				if len(hexStr) > 256 {
					return nil, -1
				}
			}
		} else {
			// ASCII/UTF-8: extract consecutive hex characters until closing quote
			for i < len(buf) {
				if buf[i] == '\'' {
					// Found closing quote
					return hexStr, i + 1
				}
				if !isHexChar(buf[i]) {
					// Non-hex character found
					return nil, -1
				}
				hexStr = append(hexStr, buf[i])
				i++

				// Safety limit
				if len(hexStr) > 256 {
					return nil, -1
				}
			}
		}

		return nil, -1
	}

	// Scan for the pattern "x'" followed by hex characters
	for i := 0; i < searchSize-10; i++ {
		// Look for "x'" pattern (UTF-8: 0x78 0x27, UTF-16LE: 0x78 0x00 0x27 0x00)
		if buffer[i] == 0x78 {
			// Check for ASCII/UTF-8 format: x'
			if i+1 < len(buffer) && buffer[i+1] == 0x27 {
				hexStr, endPos := extractHexString(buffer, i+2, false)
				if hexStr != nil && len(hexStr) >= 64 && len(hexStr) <= 192 {
					attemptCount++
					if attemptCount <= 10 {
						log.Printf("Attempt %d: Found WCDB pattern at offset 0x%X (ASCII, length: %d)", attemptCount, i, len(hexStr))
					}

					// For verification, we need at least 64 hex chars (32 bytes) for the key
					// Extract first 64 hex chars as the candidate key
					candidateKey := make([]byte, 32)
					_, err := hex.Decode(candidateKey, hexStr[:64])
					if err == nil {
						if attemptCount <= 10 {
							log.Printf("  Key hex: %s...", string(hexStr[:16]))
						}

						// Verify the key against the database
						if checkDataBaseKeyWithIter(mediaDB, candidateKey, 256000) {
							verifiedCount++
							log.Printf("✓ Successfully verified WCDB cached key (attempt %d, verified %d, length: %d)", attemptCount, verifiedCount, len(hexStr))
							return hex.EncodeToString(candidateKey)
						} else if attemptCount <= 10 {
							log.Printf("× Key verification failed for attempt %d (length: %d)", attemptCount, len(hexStr))
						}
					} else if attemptCount <= 10 {
						log.Printf("× Failed to decode hex at attempt %d: %v", attemptCount, err)
					}

					// Skip to after the closing quote
					if endPos > 0 {
						i = endPos
					}
				} else if hexStr != nil && attemptCount <= 5 {
					// Found pattern but wrong length
					log.Printf("Found x' pattern at offset 0x%X but invalid length: %d (require 64-192)", i, len(hexStr))
				}
			}

			// Check for UTF-16LE format: x\x0'\x0 (0x78 0x00 0x27 0x00)
			if i+3 < len(buffer) && buffer[i+1] == 0x00 && buffer[i+2] == 0x27 && buffer[i+3] == 0x00 {
				hexStr, endPos := extractHexString(buffer, i+4, true)
				if hexStr != nil && len(hexStr) >= 64 && len(hexStr) <= 192 {
					attemptCount++
					if attemptCount <= 10 {
						log.Printf("Attempt %d: Found WCDB pattern at offset 0x%X (UTF-16, length: %d)", attemptCount, i, len(hexStr))
					}

					// Extract first 64 hex chars as the candidate key
					candidateKey := make([]byte, 32)
					_, err := hex.Decode(candidateKey, hexStr[:64])
					if err == nil {
						if attemptCount <= 10 {
							log.Printf("  Key hex: %s...", string(hexStr[:16]))
						}

						// Verify the key against the database
						if checkDataBaseKeyWithIter(mediaDB, candidateKey, 256000) {
							verifiedCount++
							log.Printf("✓ Successfully verified WCDB cached key (attempt %d, verified %d, length: %d)", attemptCount, verifiedCount, len(hexStr))
							return hex.EncodeToString(candidateKey)
						} else if attemptCount <= 10 {
							log.Printf("× Key verification failed for attempt %d (length: %d)", attemptCount, len(hexStr))
						}
					} else if attemptCount <= 10 {
						log.Printf("× Failed to decode hex at attempt %d: %v", attemptCount, err)
					}

					// Skip to after the closing quote
					if endPos > 0 {
						i = endPos
					}
				} else if hexStr != nil && attemptCount <= 5 {
					log.Printf("Found x' pattern at offset 0x%X but invalid length: %d (require 64-192)", i, len(hexStr))
				}
			}
		}
	}

	log.Printf("WCDB cached key scan completed: %d attempts, %d verified", attemptCount, verifiedCount)
	return ""
}

// checkDataBaseKeyWithIter verifies a database key with specified PBKDF2 iteration count
// SQLCipher 4.0 uses 256,000 iterations (vs 64,000 in SQLCipher 3.x)
func checkDataBaseKeyWithIter(path string, password []byte, iterations int) bool {
	fp, err := os.Open(path)
	if err != nil {
		return false
	}
	defer fp.Close()

	fpReader := bufio.NewReaderSize(fp, defaultPageSize*100)
	buffer := make([]byte, defaultPageSize)

	n, err := fpReader.Read(buffer)
	if err != nil && n != defaultPageSize {
		return false
	}

	salt := buffer[:16]
	key := pbkdf2HMAC(password, salt, iterations, keySize)

	page1 := buffer[16:defaultPageSize]
	macSalt := xorBytes(salt, 0x3a)
	macKey := pbkdf2HMAC(key, macSalt, 2, keySize)

	hashMac := hmac.New(sha1.New, macKey)
	hashMac.Write(page1[:len(page1)-32])
	hashMac.Write([]byte{1, 0, 0, 0})

	return hmac.Equal(hashMac.Sum(nil), page1[len(page1)-32:len(page1)-12])
}

func hasDeviceSybmol(buffer []byte) int {
	sybmols := [...][]byte{
		{'a', 'n', 'd', 'r', 'o', 'i', 'd', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00},
		{'p', 'a', 'd', '-', 'a', 'n', 'd', 'r', 'o', 'i', 'd', 0x00, 0x00, 0x00, 0x00, 0x00, 0x0B, 0x00, 0x00, 0x00},
		{'i', 'p', 'h', 'o', 'n', 'e', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00},
		{'i', 'p', 'a', 'd', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00},
		{'O', 'H', 'O', 'S', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00},
	}
	for _, syb := range sybmols {
		if index := bytes.Index(buffer, syb); index != -1 {
			return index
		}
	}

	return -1
}

func findDBKeyPtr(buffer []byte, is64Bits bool) [][]byte {
	keys := make([][]byte, 0)
	step := 8
	keyLen := []byte{0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	if !is64Bits {
		keyLen = keyLen[:4]
		step = 4
	}

	offset := len(buffer) - step
	for {
		if bytes.Contains(buffer[offset:offset+step], keyLen) {
			keys = append(keys, buffer[offset-step:offset])
		}

		offset -= step
		if offset <= 0 {
			break
		}
	}

	return keys
}

func findDBkey(handle windows.Handle, path string, keys [][]byte) (string, error) {
	var keyAddrPtr uint64
	addrBuffer := make([]byte, 0x08)
	for _, key := range keys {
		copy(addrBuffer, key)
		err := binary.Read(bytes.NewReader(addrBuffer), binary.LittleEndian, &keyAddrPtr)
		if err != nil {
			log.Println("binary.Read:", err)
			continue
		}
		if keyAddrPtr == 0x00 {
			continue
		}
		log.Printf("keyAddrPtr: 0x%X\n", keyAddrPtr)
		keyBuffer := make([]byte, 0x20)
		err = windows.ReadProcessMemory(handle, uintptr(keyAddrPtr), &keyBuffer[0], uintptr(len(keyBuffer)), nil)
		if err != nil {
			// fmt.Println("Error ReadProcessMemory:", err)
			continue
		}
		if checkDataBaseKey(path, keyBuffer) {
			return hex.EncodeToString(keyBuffer), nil
		}
	}

	return "", errors.New("not found key")
}

func checkDataBaseKey(path string, password []byte) bool {
	fp, err := os.Open(path)
	if err != nil {
		return false
	}
	defer fp.Close()

	fpReader := bufio.NewReaderSize(fp, defaultPageSize*100)

	buffer := make([]byte, defaultPageSize)

	n, err := fpReader.Read(buffer)
	if err != nil && n != defaultPageSize {
		log.Println("read failed:", err, n)
		return false
	}

	salt := buffer[:16]
	key := pbkdf2HMAC(password, salt, defaultIter, keySize)

	page1 := buffer[16:defaultPageSize]

	macSalt := xorBytes(salt, 0x3a)
	macKey := pbkdf2HMAC(key, macSalt, 2, keySize)

	hashMac := hmac.New(sha1.New, macKey)
	hashMac.Write(page1[:len(page1)-32])
	hashMac.Write([]byte{1, 0, 0, 0})

	return hmac.Equal(hashMac.Sum(nil), page1[len(page1)-32:len(page1)-12])
}

func (info WeChatInfo) String() string {
	return fmt.Sprintf("PID: %d\nVersion: v%s\nBaseAddr: 0x%08X\nDllSize: %d\nIs 64Bits: %v\nFilePath %s\nAcountName: %s",
		info.ProcessID, info.Version, info.DllBaseAddr, info.DllBaseSize, info.Is64Bits, info.FilePath, info.AcountName)
}

func copyFile(src, dst string) (int64, error) {
	sourceFile, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destFile.Close()

	bytesWritten, err := io.Copy(destFile, sourceFile)
	if err != nil {
		return bytesWritten, err
	}

	return bytesWritten, nil
}

func silkToMp3(amrBuf []byte, mp3Path string) error {
	amrReader := bytes.NewReader(amrBuf)

	var pcmBuffer bytes.Buffer
	sr := silk.NewWriter(&pcmBuffer)
	sr.Decoder.SetSampleRate(24000)
	amrReader.WriteTo(sr)
	sr.Close()

	if pcmBuffer.Len() == 0 {
		return errors.New("silk to mp3 failed " + mp3Path)
	}

	of, err := os.Create(mp3Path)
	if err != nil {
		return nil
	}
	defer of.Close()

	wr := lame.NewWriter(of)
	wr.Encoder.SetInSamplerate(24000)
	wr.Encoder.SetOutSamplerate(24000)
	wr.Encoder.SetNumChannels(1)
	wr.Encoder.SetQuality(5)
	// IMPORTANT!
	wr.Encoder.InitParams()

	pcmBuffer.WriteTo(wr)
	wr.Close()

	return nil
}

func getPathFileNumber(targetPath string, fileSuffix string) int64 {

	number := int64(0)
	err := filepath.Walk(targetPath, func(path string, finfo os.FileInfo, err error) error {
		if err != nil {
			log.Printf("filepath.Walk：%v\n", err)
			return err
		}
		if !finfo.IsDir() && strings.HasSuffix(path, fileSuffix) {
			number += 1
		}

		return nil
	})
	if err != nil {
		log.Println("filepath.Walk:", err)
	}

	return number
}

func ExportWeChatHeadImage(exportPath string) {
	progress := make(chan string)
	info := WeChatInfo{}

	miscDBPath := fmt.Sprintf("%s\\Msg\\Misc.db", exportPath)
	_, err := os.Stat(miscDBPath)
	if err != nil {
		log.Println("no exist:", miscDBPath)
		return
	}

	headImgPath := fmt.Sprintf("%s\\FileStorage\\HeadImage", exportPath)
	if _, err := os.Stat(headImgPath); err == nil {
		log.Println("has HeadImage")
		return
	}

	go func() {
		exportWeChatHeadImage(info, exportPath, progress)
		close(progress)
	}()

	for p := range progress {
		log.Println(p)
	}
	log.Println("ExportWeChatHeadImage done")
}
