use crate::CommonErrorKind;
pub enum CommonDefaultErrorKind {
	ConnectFail,
	Critical,
	Etc,
	ExecuteFail,
	InvalidApiCall,
	LimitSize,
	MaxSize,
	NoData,
	NoSupport,
	NotMatchArgs,
	OverFlowMemory,
	ParsingFail,
	SystemCallFail,
	ThirdLibCallFail,
}

impl CommonErrorKind for CommonDefaultErrorKind {
	fn message(&self) -> &'static str {
		match self {
			CommonDefaultErrorKind::ConnectFail => "Connect failed",
			CommonDefaultErrorKind::Critical => "critical error, need restart system",
			CommonDefaultErrorKind::Etc => "etc error",
			CommonDefaultErrorKind::ExecuteFail => "execute error failed",
			CommonDefaultErrorKind::InvalidApiCall => "invalid api call",
			CommonDefaultErrorKind::LimitSize => "limit size",
			CommonDefaultErrorKind::MaxSize => "memory is used Max size",
			CommonDefaultErrorKind::NoData => "no data",
			CommonDefaultErrorKind::NoSupport => "not support function",
			CommonDefaultErrorKind::NotMatchArgs => "args count not matching",
			CommonDefaultErrorKind::OverFlowMemory => "overflow memory size",
			CommonDefaultErrorKind::ParsingFail => "parsing failed",
			CommonDefaultErrorKind::SystemCallFail => "System or Std Lib call failed",
			CommonDefaultErrorKind::ThirdLibCallFail => "ThirdLib call failed",
		}
	}
	fn name(&self) -> &'static str {
		match self {
			CommonDefaultErrorKind::ConnectFail => "CommonDefaultErrorKind::ConnectFail",
			CommonDefaultErrorKind::Critical => "CommonDefaultErrorKind::Critical",
			CommonDefaultErrorKind::Etc => "CommonDefaultErrorKind::Etc",
			CommonDefaultErrorKind::ExecuteFail => "CommonDefaultErrorKind::ExecuteFail",
			CommonDefaultErrorKind::InvalidApiCall => "CommonDefaultErrorKind::InvalidApiCall",
			CommonDefaultErrorKind::LimitSize => "CommonDefaultErrorKind::LimitSize",
			CommonDefaultErrorKind::MaxSize => "CommonDefaultErrorKind::MaxSize",
			CommonDefaultErrorKind::NoData => "CommonDefaultErrorKind::NoData",
			CommonDefaultErrorKind::NoSupport => "CommonDefaultErrorKind::NoSupport",
			CommonDefaultErrorKind::NotMatchArgs => "CommonDefaultErrorKind::NotMatchArgs",
			CommonDefaultErrorKind::OverFlowMemory => "CommonDefaultErrorKind::OverFlowMemory",
			CommonDefaultErrorKind::ParsingFail => "CommonDefaultErrorKind::ParsingFail",
			CommonDefaultErrorKind::SystemCallFail => "CommonDefaultErrorKind::SystemCallFail",
			CommonDefaultErrorKind::ThirdLibCallFail => "CommonDefaultErrorKind::ThirdLibCallFail",
		}
	}
}
