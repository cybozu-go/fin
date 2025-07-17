package controller

import (
	_ "embed"
)

var (
	//go:embed script/toolbox.sh
	embeddedToolboxScript string
)
