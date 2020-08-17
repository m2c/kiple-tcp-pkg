package slog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"time"
)

var log *zap.SugaredLogger

func init() {
	encoder := getEncoder()
	core := zapcore.NewTee(
		zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), zap.DebugLevel),
	)
	// develop mode
	caller := zap.AddCaller()
	// open the code line
	development := zap.Development()
	logger := zap.New(core, caller, development, zap.AddCallerSkip(1))
	log = logger.Sugar()
	//set iris log level
}

/**
 * time format
 */
func customTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("[2006-01-02 15:04:05]"))
}

/**
 * get zap log encoder
 */
func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = customTimeEncoder
	encoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	encoderConfig.LineEnding = zapcore.DefaultLineEnding
	return zapcore.NewConsoleEncoder(encoderConfig)
}

func Info(args ...interface{}) {
	log.Info(args...)
}
func Infof(template string, args ...interface{}) {
	log.Infof(template, args...)
}
func Debug(args ...interface{}) {
	log.Debug(args...)
}
func Debugf(template string, args ...interface{}) {
	log.Debugf(template, args...)
}
func Error(args ...interface{}) {
	log.Error(args...)
}
func Errorf(template string, args ...interface{}) {
	log.Errorf(template, args...)
}
