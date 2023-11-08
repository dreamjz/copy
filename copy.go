package copy

import (
	"context"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type timespec struct {
	Mtime time.Time
	Atime time.Time
	Ctime time.Time
}

// Copy copies src to dest, doesn't matter if src is a directory or a file.
func Copy(src, dest string, opts ...Options) error {
	// 确保相关字段非默认值
	opt := assureOptions(src, dest, opts...)
	// 工作协程数量，0 or 1 不开启多协程
	// 大于 1 开启多协程并发处理
	if opt.NumOfWorkers > 1 {
		// 设置信号量
		opt.intent.sem = semaphore.NewWeighted(opt.NumOfWorkers) // 设置核心数，用于控制并行任务
		// 设置 Context
		opt.intent.ctx = context.Background()
	}
	// 设置了文件系统，则不使用默认的当前 OS 的文件系统
	if opt.FS != nil {
		// 获取文件信息
		info, err := fs.Stat(opt.FS, src)
		if err != nil {
			// 出现错误，进行错误处理
			// 若 opt.OnError 不为空，则调用用户定义的函数进行处理
			// 否则直接返回 err
			return onError(src, dest, err, opt)
		}
		// 选择合适的函数开始处理
		return switchboard(src, dest, info, opt)
	}
	// 获取文件信息
	info, err := os.Lstat(src)
	if err != nil {
		return onError(src, dest, err, opt)
	}
	return switchboard(src, dest, info, opt)
}

// switchboard switches proper copy functions regarding file type, etc...
// If there would be anything else here, add a case to this switchboard.
func switchboard(src, dest string, info os.FileInfo, opt Options) (err error) {
	// 文件为 Device 设备类型 并且 opt 中未开启特殊文件copy
	// 返回错误，但是这里的错误为 nil？
	if info.Mode()&os.ModeDevice != 0 && !opt.Specials {
		return onError(src, dest, err, opt)
	}

	switch {
	case info.Mode()&os.ModeSymlink != 0: // 文件链接
		err = onsymlink(src, dest, opt)
	case info.IsDir(): // 目录
		err = dcopy(src, dest, info, opt)
	case info.Mode()&os.ModeNamedPipe != 0: // Pipeline
		err = pcopy(dest, info)
	default: // 默认为普通文件
		err = fcopy(src, dest, info, opt)
	}

	// 无对应模式，则返回错误
	return onError(src, dest, err, opt)
}

// copyNextOrSkip decide if this src should be copied or not.
// Because this "copy" could be called recursively,
// "info" MUST be given here, NOT nil.
func copyNextOrSkip(src, dest string, info os.FileInfo, opt Options) error {
	if opt.Skip != nil {
		skip, err := opt.Skip(info, src, dest)
		if err != nil {
			return err
		}
		if skip {
			return nil
		}
	}
	return switchboard(src, dest, info, opt)
}

// fcopy is for just a file,
// with considering existence of parent directory
// and file permission.
func fcopy(src, dest string, info os.FileInfo, opt Options) (err error) {

	var readcloser io.ReadCloser
	// 使用对应的文件系统打开文件
	if opt.FS != nil {
		readcloser, err = opt.FS.Open(src)
	} else {
		readcloser, err = os.Open(src)
	}
	if err != nil {
		if os.IsNotExist(err) { // 文件不存在，返回 nil？
			return nil
		}
		return // 返回错误
	}
	// 确保文件关闭
	// 这里 &err 使用指针
	// defer 函数的参数会进行预计算，所以使用指针
	// 也可以使用匿名函数
	// defer func() {
	//	fclose(readcloser, err) // 当然这里对应的函数也要改
	// }()
	defer fclose(readcloser, &err)

	// 创建目标文件夹
	// filepath.Dir 获取文件夹路径
	// os.MkdirAll 创建所有的子文件夹
	// os.ModePerm 0777
	if err = os.MkdirAll(filepath.Dir(dest), os.ModePerm); err != nil {
		return
	}

	// os.Create 创建目标文件
	// 已存在则清空原文件
	// 不存在则创建文件，权限 0666
	// 实际底层调用为
	// os.OpenFile(name, O_RDWR|O_CREATE|O_TRUNC, 0666)
	f, err := os.Create(dest)
	if err != nil {
		return
	}
	defer fclose(f, &err)

	// 调用用户自定义函数，改变文件权限
	chmodfunc, err := opt.PermissionControl(info, dest)
	if err != nil {
		return err
	}
	// 这里把当前的 err 交给自定义函数处理
	chmodfunc(&err)

	var buf []byte = nil // 缓冲
	var w io.Writer = f
	var r io.Reader = readcloser

	// 采用自定义 Reader
	if opt.WrapReader != nil {
		r = opt.WrapReader(r)
	}

	// 使用自定义的缓冲大小
	if opt.CopyBufferSize != 0 {
		buf = make([]byte, opt.CopyBufferSize)
		// Disable using `ReadFrom` by io.CopyBuffer.
		// See https://github.com/otiai10/copy/pull/60#discussion_r627320811 for more details.
		w = struct{ io.Writer }{f}
		// r = struct{ io.Reader }{s}
	}

	if _, err = io.CopyBuffer(w, r, buf); err != nil { // 使用缓冲进行复制
		return err
	}

	if opt.Sync { // 立刻写入硬盘
		err = f.Sync()
	}

	if opt.PreserveOwner { // 保留 Owner
		if err := preserveOwner(src, dest, info); err != nil {
			return err
		}
	}
	if opt.PreserveTimes { // 保留访问和修改时间
		if err := preserveTimes(info, dest); err != nil {
			return err
		}
	}

	return
}

// dcopy is for a directory,
// with scanning contents inside the directory
// and pass everything to "copy" recursively.
func dcopy(srcdir, destdir string, info os.FileInfo, opt Options) (err error) {
	// 目标已存在时，选择如何处理
	// Replace：删除目标文件夹，返回 false
	// Untouchable：什么也不做，返回 true
	// Merge：合并两个文件夹，默认行为
	if skip, err := onDirExists(opt, srcdir, destdir); err != nil {
		return err
	} else if skip { // 什么也不做，直接结束
		return nil
	}

	// Make dest dir with 0755 so that everything writable.
	// 默认函数创建文件夹权限为 0755
	chmodfunc, err := opt.PermissionControl(info, destdir)
	if err != nil {
		return err
	}
	defer chmodfunc(&err)

	var contents []os.FileInfo
	if opt.FS != nil { // 使用自定义文件系统
		entries, err := fs.ReadDir(opt.FS, srcdir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			info, err := e.Info()
			if err != nil {
				return err
			}
			contents = append(contents, info)
		}
	} else { // 使用操作系统的文件系统
		contents, err = ioutil.ReadDir(srcdir)
	}

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return
	}

	// 决定复制方式，并发还是串行
	if yes, err := shouldCopyDirectoryConcurrent(opt, srcdir, destdir); err != nil {
		return err
	} else if yes { // 并发复制
		if err := dcopyConcurrent(srcdir, destdir, contents, opt); err != nil {
			return err
		}
	} else { // 串行复制
		if err := dcopySequential(srcdir, destdir, contents, opt); err != nil {
			return err
		}
	}

	if opt.PreserveTimes {
		if err := preserveTimes(info, destdir); err != nil {
			return err
		}
	}

	if opt.PreserveOwner {
		if err := preserveOwner(srcdir, destdir, info); err != nil {
			return err
		}
	}

	return
}

func dcopySequential(srcdir, destdir string, contents []os.FileInfo, opt Options) error {
	for _, content := range contents {
		// 构建 源/目标 文件路径
		cs, cd := filepath.Join(srcdir, content.Name()), filepath.Join(destdir, content.Name())
		// 决定拷贝还是跳过
		if err := copyNextOrSkip(cs, cd, content, opt); err != nil {
			// If any error, exit immediately
			return err
		}
	}
	return nil
}

// Copy this directory concurrently regarding semaphore of opt.intent
func dcopyConcurrent(srcdir, destdir string, contents []os.FileInfo, opt Options) error {
	group, ctx := errgroup.WithContext(opt.intent.ctx)
	getRoutine := func(cs, cd string, content os.FileInfo) func() error {
		return func() error {
			if content.IsDir() {
				return copyNextOrSkip(cs, cd, content, opt) // 是目录则递归处理
			}
			if err := opt.intent.sem.Acquire(ctx, 1); err != nil { // 请求信号量
				return err
			}
			err := copyNextOrSkip(cs, cd, content, opt)
			opt.intent.sem.Release(1) // 释放信号量
			return err
		}
	}
	for _, content := range contents {
		csd := filepath.Join(srcdir, content.Name())
		cdd := filepath.Join(destdir, content.Name())
		group.Go(getRoutine(csd, cdd, content)) // 启用子协程处理
	}
	return group.Wait() // 等待处理完毕
}

func onDirExists(opt Options, srcdir, destdir string) (bool, error) {
	_, err := os.Stat(destdir)
	if err == nil && opt.OnDirExists != nil && destdir != opt.intent.dest {
		switch opt.OnDirExists(srcdir, destdir) {
		case Replace:
			if err := os.RemoveAll(destdir); err != nil {
				return false, err
			}
		case Untouchable:
			return true, nil
		} // case "Merge" is default behaviour. Go through.
	} else if err != nil && !os.IsNotExist(err) {
		return true, err // Unwelcome error type...!
	}
	return false, nil
}

func onsymlink(src, dest string, opt Options) error {
	switch opt.OnSymlink(src) {
	case Shallow: // 浅复制
		if err := lcopy(src, dest); err != nil {
			return err
		}
		if opt.PreserveTimes {
			return preserveLtimes(src, dest)
		}
		return nil
	case Deep: // 深复制
		orig, err := os.Readlink(src) // 获取链接的目标
		if err != nil {
			return err
		}
		info, err := os.Lstat(orig)
		if err != nil {
			return err
		}
		return copyNextOrSkip(orig, dest, info, opt)
	case Skip:
		fallthrough
	default:
		return nil // do nothing
	}
}

// lcopy is for a symlink,
// with just creating a new symlink by replicating src symlink.
func lcopy(src, dest string) error {
	src, err := os.Readlink(src) // 获取链接的目标文件
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return os.Symlink(src, dest)
}

// fclose ANYHOW closes file,
// with asiging error raised during Close,
// BUT respecting the error already reported.
func fclose(f io.Closer, reported *error) {
	// 关闭文件
	// 优先返回已出现的错误
	if err := f.Close(); *reported == nil {
		*reported = err
	}
}

// onError lets caller to handle errors
// occured when copying a file.
func onError(src, dest string, err error, opt Options) error {
	if opt.OnError == nil {
		return err
	}

	return opt.OnError(src, dest, err)
}
