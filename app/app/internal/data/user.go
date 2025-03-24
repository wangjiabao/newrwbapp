package data

import (
	"context"
	"dhb/app/app/internal/biz"
	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
	"strconv"
	"strings"
	"time"
)

type User struct {
	ID                     int64     `gorm:"primarykey;type:int"`
	Address                string    `gorm:"type:varchar(100)"`
	Password               string    `gorm:"type:varchar(100)"`
	AddressTwo             string    `gorm:"type:varchar(100)"`
	PrivateKey             string    `gorm:"type:varchar(200)"`
	AddressThree           string    `gorm:"type:varchar(100)"`
	PrivateKeyThree        string    `gorm:"type:varchar(400)"`
	WordThree              string    `gorm:"type:varchar(200)"`
	Undo                   int64     `gorm:"type:int;not null"`
	Amount                 uint64    `gorm:"type:bigint"`
	AmountBiw              uint64    `gorm:"type:bigint"`
	CreatedAt              time.Time `gorm:"type:datetime;not null"`
	UpdatedAt              time.Time `gorm:"type:datetime;not null"`
	IsDelete               int64     `gorm:"type:int;not null"`
	Out                    int64     `gorm:"type:int;not null"`
	OutRate                int64     `gorm:"type:int;not null"`
	Lock                   int64     `gorm:"type:int;not null"`
	Vip                    int64     `gorm:"type:int;not null"`
	LockReward             int64     `gorm:"type:int;not null"`
	RecommendLevel         int64     `gorm:"type:int;not null"`
	AmountUsdt             float64   `gorm:"type:decimal(65,20);not null"`
	MyTotalAmount          float64   `gorm:"type:decimal(65,20);not null"`
	AmountUsdtGet          float64   `gorm:"type:decimal(65,20);not null"`
	AmountRecommendUsdtGet float64   `gorm:"type:decimal(65,20);not null"`
	Last                   uint64    `gorm:"type:bigint;not null"`
}

type Trade struct {
	ID           int64     `gorm:"primarykey;type:int"`
	UserId       int64     `gorm:"type:int"`
	AmountCsd    int64     `gorm:"type:bigint"`
	RelAmountCsd int64     `gorm:"type:bigint"`
	AmountHbs    int64     `gorm:"type:bigint"`
	RelAmountHbs int64     `gorm:"type:bigint"`
	CsdReward    int64     `gorm:"type:bigint"`
	Status       string    `gorm:"type:varchar(45);not null"`
	CreatedAt    time.Time `gorm:"type:datetime;not null"`
	UpdatedAt    time.Time `gorm:"type:datetime;not null"`
}

type UserInfo struct {
	ID               int64     `gorm:"primarykey;type:int"`
	UserId           int64     `gorm:"type:int;not null"`
	Vip              int64     `gorm:"type:int;not null"`
	UseVip           int64     `gorm:"type:int;not null"`
	HistoryRecommend int64     `gorm:"type:int;not null"`
	CreatedAt        time.Time `gorm:"type:datetime;not null"`
	UpdatedAt        time.Time `gorm:"type:datetime;not null"`
	TeamCsdBalance   int64     `gorm:"type:bigint;not null"`
}

type UserRecommend struct {
	ID            int64     `gorm:"primarykey;type:int"`
	UserId        int64     `gorm:"type:int;not null"`
	Total         int64     `gorm:"type:bigint"`
	RecommendCode string    `gorm:"type:varchar(10000);not null"`
	CreatedAt     time.Time `gorm:"type:datetime;not null"`
	UpdatedAt     time.Time `gorm:"type:datetime;not null"`
}

type BalanceReward struct {
	ID             int64     `gorm:"primarykey;type:int"`
	UserId         int64     `gorm:"type:int;not null"`
	Amount         int64     `gorm:"type:bigint;not null"`
	Status         int64     `gorm:"type:int;not null"`
	H              int64     `gorm:"type:int;not null"`
	M              int64     `gorm:"type:int;not null"`
	SetDate        time.Time `gorm:"type:datetime;not null"`
	LastRewardDate time.Time `gorm:"type:datetime;not null"`
	CreatedAt      time.Time `gorm:"type:datetime;not null"`
	UpdatedAt      time.Time `gorm:"type:datetime;not null"`
}

type UserRecommendArea struct {
	ID            int64     `gorm:"primarykey;type:int"`
	RecommendCode string    `gorm:"type:varchar(10000);not null"`
	Version       int64     `gorm:"type:int;not null"`
	Num           int64     `gorm:"type:int;not null"`
	CreatedAt     time.Time `gorm:"type:datetime;not null"`
	UpdatedAt     time.Time `gorm:"type:datetime;not null"`
}

type UserArea struct {
	ID         int64     `gorm:"primarykey;type:int"`
	UserId     int64     `gorm:"type:int;not null"`
	Level      int64     `gorm:"type:int;not null"`
	Amount     int64     `gorm:"type:bigint;not null"`
	SelfAmount int64     `gorm:"type:bigint;not null"`
	CreatedAt  time.Time `gorm:"type:datetime;not null"`
	UpdatedAt  time.Time `gorm:"type:datetime;not null"`
}

type UserCurrentMonthRecommend struct {
	ID              int64     `gorm:"primarykey;type:int"`
	UserId          int64     `gorm:"type:int;not null"`
	RecommendUserId int64     `gorm:"type:int;not null"`
	Date            time.Time `gorm:"type:datetime;not null"`
	CreatedAt       time.Time `gorm:"type:datetime;not null"`
	UpdatedAt       time.Time `gorm:"type:datetime;not null"`
}

type Config struct {
	ID        int64     `gorm:"primarykey;type:int"`
	Name      string    `gorm:"type:varchar(45);not null"`
	KeyName   string    `gorm:"type:varchar(45);not null"`
	Value     string    `gorm:"type:varchar(1000);not null"`
	CreatedAt time.Time `gorm:"type:datetime;not null"`
	UpdatedAt time.Time `gorm:"type:datetime;not null"`
}

type UserBalance struct {
	ID                  int64     `gorm:"primarykey;type:int"`
	UserId              int64     `gorm:"type:int"`
	BalanceUsdt         int64     `gorm:"type:bigint"`
	BalanceUsdtNew      int64     `gorm:"type:bigint"`
	BalanceDhb          int64     `gorm:"type:bigint"`
	CreatedAt           time.Time `gorm:"type:datetime;not null"`
	UpdatedAt           time.Time `gorm:"type:datetime;not null"`
	AreaTotal           int64     `gorm:"type:bigint"`
	RecommendTotal      int64     `gorm:"type:bigint"`
	LocationTotal       int64     `gorm:"type:bigint"`
	FourTotal           int64     `gorm:"type:bigint"`
	BalanceC            int64     `gorm:"type:bigint"`
	AreaTotalFloat      float64   `gorm:"type:decimal(65,20);not null"`
	AreaTotalFloatTwo   float64   `gorm:"type:decimal(65,20);not null"`
	AreaTotalFloatThree float64   `gorm:"type:decimal(65,20);not null"`
	RecommendTotalFloat float64   `gorm:"type:decimal(65,20);not null"`
	LocationTotalFloat  float64   `gorm:"type:decimal(65,20);not null"`
	BalanceUsdtFloat    float64   `gorm:"type:decimal(65,20);not null"`
	BalanceKsdtFloat    float64   `gorm:"type:decimal(65,20);not null"`
	BalanceRawFloat     float64   `gorm:"type:decimal(65,20);not null"`
}

type Withdraw struct {
	ID              int64     `gorm:"primarykey;type:int"`
	UserId          int64     `gorm:"type:int"`
	Amount          int64     `gorm:"type:bigint;not null"`
	RelAmount       int64     `gorm:"type:bigint;not null"`
	AmountNew       float64   `gorm:"type:decimal(65,20);not null"`
	RelAmountNew    float64   `gorm:"type:decimal(65,20);not null"`
	Status          string    `gorm:"type:varchar(45);not null"`
	Type            string    `gorm:"type:varchar(45);not null"`
	BalanceRecordId int64     `gorm:"type:int;not null"`
	Address         string    `gorm:"type:varchar(45);not null"`
	CreatedAt       time.Time `gorm:"type:datetime;not null"`
	UpdatedAt       time.Time `gorm:"type:datetime;not null"`
}

type UserBalanceRecord struct {
	ID           int64     `gorm:"primarykey;type:int"`
	UserId       int64     `gorm:"type:int"`
	Balance      int64     `gorm:"type:bigint"`
	Amount       int64     `gorm:"type:bigint"`
	Type         string    `gorm:"type:varchar(45);not null"`
	CoinType     string    `gorm:"type:varchar(45);not null"`
	CreatedAt    time.Time `gorm:"type:datetime;not null"`
	UpdatedAt    time.Time `gorm:"type:datetime;not null"`
	BalanceNew   float64   `gorm:"type:decimal(65,20);not null"`
	AmountNew    float64   `gorm:"type:decimal(65,20);not null"`
	AmountNewTwo float64   `gorm:"type:decimal(65,20);not null"`
}

type Stake struct {
	ID        int64     `gorm:"primarykey;type:int"`
	UserId    int64     `gorm:"type:int;not null"`
	Status    int64     `gorm:"type:int;not null"`
	Day       int64     `gorm:"type:int;not null"`
	Amount    float64   `gorm:"type:decimal(65,20);not null"`
	Reward    float64   `gorm:"type:decimal(65,20);not null"`
	CreatedAt time.Time `gorm:"type:datetime;not null"`
	UpdatedAt time.Time `gorm:"type:datetime;not null"`
}

type Reward struct {
	ID               int64     `gorm:"primarykey;type:int"`
	UserId           int64     `gorm:"type:int;not null"`
	Amount           int64     `gorm:"type:bigint;not null"`
	AmountB          int64     `gorm:"type:bigint;not null"`
	BalanceRecordId  int64     `gorm:"type:int;not null"`
	Type             string    `gorm:"type:varchar(45);not null"`
	TypeRecordId     int64     `gorm:"type:int;not null"`
	Reason           string    `gorm:"type:varchar(45);not null"`
	ReasonLocationId int64     `gorm:"type:int;not null"`
	LocationType     string    `gorm:"type:varchar(45);not null"`
	CreatedAt        time.Time `gorm:"type:datetime;not null"`
	UpdatedAt        time.Time `gorm:"type:datetime;not null"`
	Address          string    `gorm:"type:varchar(100);not null"`
	AmountNew        float64   `gorm:"type:decimal(65,20);not null"`
	AmountNewTwo     float64   `gorm:"type:decimal(65,20);not null"`
}

type UserRepo struct {
	data *Data
	log  *log.Helper
}

type ConfigRepo struct {
	data *Data
	log  *log.Helper
}

type UserInfoRepo struct {
	data *Data
	log  *log.Helper
}

type UserRecommendRepo struct {
	data *Data
	log  *log.Helper
}

type UserCurrentMonthRecommendRepo struct {
	data *Data
	log  *log.Helper
}

type UserBalanceRepo struct {
	data *Data
	log  *log.Helper
}

func NewUserRepo(data *Data, logger log.Logger) biz.UserRepo {
	return &UserRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

func NewUserInfoRepo(data *Data, logger log.Logger) biz.UserInfoRepo {
	return &UserInfoRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

func NewConfigRepo(data *Data, logger log.Logger) biz.ConfigRepo {
	return &ConfigRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

func NewUserBalanceRepo(data *Data, logger log.Logger) biz.UserBalanceRepo {
	return &UserBalanceRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

func NewUserRecommendRepo(data *Data, logger log.Logger) biz.UserRecommendRepo {
	return &UserRecommendRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

func NewUserCurrentMonthRecommendRepo(data *Data, logger log.Logger) biz.UserCurrentMonthRecommendRepo {
	return &UserCurrentMonthRecommendRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

// GetUserByAddress .
func (u *UserRepo) GetUserByAddress(ctx context.Context, address string) (*biz.User, error) {
	var user User
	if err := u.data.db.Where(&User{Address: address}).Table("user").First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		return nil, errors.New(500, "USER ERROR", err.Error())
	}

	return &biz.User{
		ID:         user.ID,
		Address:    user.Address,
		Password:   user.Password,
		IsDelete:   user.IsDelete,
		AmountUsdt: user.AmountUsdt,
		OutRate:    uint64(user.OutRate),
		Lock:       user.Lock,
	}, nil
}

// GetConfigByKeys .
func (c *ConfigRepo) GetConfigByKeys(ctx context.Context, keys ...string) ([]*biz.Config, error) {
	var configs []*Config
	res := make([]*biz.Config, 0)
	if err := c.data.db.Where("key_name IN (?)", keys).Table("config").Find(&configs).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("CONFIG_NOT_FOUND", "config not found")
		}

		return nil, errors.New(500, "Config ERROR", err.Error())
	}

	for _, config := range configs {
		res = append(res, &biz.Config{
			ID:      config.ID,
			KeyName: config.KeyName,
			Name:    config.Name,
			Value:   config.Value,
		})
	}

	return res, nil
}

// GetConfigs .
func (c *ConfigRepo) GetConfigs(ctx context.Context) ([]*biz.Config, error) {
	var configs []*Config
	res := make([]*biz.Config, 0)
	if err := c.data.db.Table("config").Find(&configs).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("CONFIG_NOT_FOUND", "config not found")
		}

		return nil, errors.New(500, "Config ERROR", err.Error())
	}

	for _, config := range configs {
		res = append(res, &biz.Config{
			ID:      config.ID,
			KeyName: config.KeyName,
			Name:    config.Name,
			Value:   config.Value,
		})
	}

	return res, nil
}

// UpdateConfig .
func (c *ConfigRepo) UpdateConfig(ctx context.Context, id int64, value string) (bool, error) {
	var config Config
	config.Value = value

	res := c.data.DB(ctx).Table("config").Where("id=?", id).Updates(&config)
	if res.Error != nil {
		return false, errors.New(500, "UPDATE_USER_INFO_ERROR", "用户信息修改失败")
	}

	return true, nil
}

// UpdateUserMyTotalAmount .
func (u *UserRepo) UpdateUserMyTotalAmount(ctx context.Context, userId int64, amountUsdt float64) error {
	res := u.data.DB(ctx).Table("user").Where("id=?", userId).
		Updates(map[string]interface{}{"my_total_amount": gorm.Expr("my_total_amount + ?", amountUsdt)})
	if res.Error != nil {
		return errors.New(500, "UPDATE_USER_ERROR", "用户信息修改失败")
	}

	return nil
}

// UpdateUserRewardRecommend2 .
func (u *UserRepo) UpdateUserRewardRecommend2(ctx context.Context, userId, recommendUserId int64, userAddress string, amountUsdt float64) (int64, error) {
	var err error

	if err = u.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{
			"balance_ksdt_float":    gorm.Expr("balance_ksdt_float + ?", amountUsdt),
			"recommend_level_float": gorm.Expr("recommend_level_float + ?", amountUsdt),
		}).Error; nil != err {
		return 0, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = u.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.BalanceNew = userBalance.BalanceUsdtFloat
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "reward"
	userBalanceRecode.CoinType = "ksdt"
	userBalanceRecode.AmountNew = amountUsdt
	err = u.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	var reward Reward
	reward.UserId = userBalance.UserId
	reward.AmountNew = amountUsdt
	reward.BalanceRecordId = userBalanceRecode.ID
	reward.ReasonLocationId = recommendUserId
	reward.Address = userAddress
	reward.Type = "system_reward_recommend_level" // 本次分红的行为类型
	reward.Reason = "recommend_level"
	err = u.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// UpdateUserMyTotalAmountSub .
func (u *UserRepo) UpdateUserMyTotalAmountSub(ctx context.Context, userId int64, amountUsdt float64) error {
	res := u.data.DB(ctx).Table("user").Where("id=?", userId).
		Updates(map[string]interface{}{"my_total_amount": gorm.Expr("my_total_amount - ?", amountUsdt)})
	if res.Error != nil {
		return errors.New(500, "UPDATE_USER_ERROR", "用户信息修改失败")
	}

	return nil
}

// UpdateUserRewardAreaTwo .
func (u *UserRepo) UpdateUserRewardAreaTwo(ctx context.Context, userId int64, amountUsdt float64, amountUsdtTotal float64, stop bool, level, i int64, address string) (int64, error) {
	var err error

	if stop {
		res := u.data.DB(ctx).Table("user").Where("id=?", userId).
			Updates(map[string]interface{}{"amount_usdt_get": 0, "amount_usdt": 0, "last": 0, "amount_recommend_usdt_get": 0, "out_rate": gorm.Expr("out_rate + ?", 1)})
		if res.Error != nil {
			return 0, errors.New(500, "UPDATE_USER_ERROR", "用户信息修改失败")
		}

		var rewardStop Reward
		rewardStop.UserId = userId
		rewardStop.AmountNew = amountUsdtTotal
		rewardStop.Type = "out"   // 本次分红的行为类型
		rewardStop.Reason = "out" // 给我分红的理由
		err = u.data.DB(ctx).Table("reward").Create(&rewardStop).Error
		if err != nil {
			return 0, err
		}
	} else {
		res := u.data.DB(ctx).Table("user").Where("id=?", userId).
			Updates(map[string]interface{}{"amount_usdt_get": gorm.Expr("amount_usdt_get + ?", amountUsdt)})
		if res.Error != nil {
			return 0, errors.New(500, "UPDATE_USER_ERROR", "用户信息修改失败")
		}
	}

	if err = u.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_usdt_float": gorm.Expr("balance_usdt_float + ?", amountUsdt), "area_total_float_two": gorm.Expr("area_total_float_two + ?", amountUsdt)}).Error; nil != err {
		return 0, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = u.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "reward"
	userBalanceRecode.CoinType = "usdt"

	userBalanceRecode.AmountNew = amountUsdt
	err = u.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	var reward Reward
	reward.UserId = userBalance.UserId
	reward.AmountNew = amountUsdt
	reward.BalanceRecordId = level
	reward.ReasonLocationId = i
	reward.Address = address
	reward.Type = "system_reward_area_two" // 本次分红的行为类型
	reward.Reason = "area_two"
	err = u.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// UpdateUserNewTwoNew .
func (u *UserRepo) UpdateUserNewTwoNew(ctx context.Context, userId int64, amountUsdt float64, last uint64, amountRawFloat float64, coinType string) error {
	if "USDT" == coinType {
		res := u.data.DB(ctx).Table("user").Where("id=?", userId).
			Updates(map[string]interface{}{"amount_usdt": gorm.Expr("amount_usdt + ?", amountUsdt), "last": last, "updated_at": time.Now().Format("2006-01-02 15:04:05")})
		if res.Error != nil {
			return errors.New(500, "UPDATE_USER_ERROR", "用户信息修改失败")
		}

		res = u.data.DB(ctx).Table("user_balance").Where("user_id=?", userId).Where("balance_usdt_float>=?", amountRawFloat).
			Updates(map[string]interface{}{"balance_usdt_float": gorm.Expr("balance_usdt_float - ?", amountRawFloat)})
		if res.Error != nil {
			return errors.New(500, "UPDATE_USER_ERROR", "用户信息修改失败")
		}

	} else if "KSDT" == coinType {
		res := u.data.DB(ctx).Table("user").Where("id=?", userId).
			Updates(map[string]interface{}{"amount_usdt": gorm.Expr("amount_usdt + ?", amountUsdt), "last": last, "updated_at": time.Now().Format("2006-01-02 15:04:05")})
		if res.Error != nil {
			return errors.New(500, "UPDATE_USER_ERROR", "用户信息修改失败")
		}

		res = u.data.DB(ctx).Table("user_balance").Where("user_id=?", userId).Where("balance_ksdt_float>=?", amountRawFloat).
			Updates(map[string]interface{}{"balance_ksdt_float": gorm.Expr("balance_ksdt_float - ?", amountRawFloat)})
		if res.Error != nil {
			return errors.New(500, "UPDATE_USER_ERROR", "用户信息修改失败")
		}

	} else {
		return errors.New(500, "UPDATE_USER_ERROR", "未知类型，修改余额")
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.UserId = userId
	userBalanceRecode.Type = "deposit"
	userBalanceRecode.CoinType = coinType
	userBalanceRecode.AmountNew = amountRawFloat
	res := u.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode)
	if res.Error != nil {
		return errors.New(500, "CREATE_LOCATION_ERROR", "占位信息创建失败")
	}

	var (
		err    error
		reward Reward
	)

	reward.UserId = userId
	reward.AmountNew = amountUsdt
	reward.AmountNewTwo = amountRawFloat
	reward.Type = coinType // 本次分红的行为类型
	reward.TypeRecordId = userBalanceRecode.ID
	reward.Reason = "buy" // 给我分红的理由
	err = u.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return errors.New(500, "CREATE_LOCATION_ERROR", "占位信息创建失败")
	}
	return nil
}

func (u *UserRepo) GetEthUserRecordListByUserId(ctx context.Context, userId int64) ([]*biz.EthUserRecord, error) {
	var ethUserRecord []*EthUserRecord

	res := make([]*biz.EthUserRecord, 0)
	if err := u.data.DB(ctx).Table("eth_user_record").Where("user_id=?", userId).Find(&ethUserRecord).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USER_RECOMMEND_NOT_FOUND", "user recommend not found")
		}

		return nil, errors.New(500, "USER RECOMMEND ERROR", err.Error())
	}

	for _, item := range ethUserRecord {
		res = append(res, &biz.EthUserRecord{
			ID:        item.ID,
			UserId:    item.UserId,
			Hash:      item.Hash,
			Status:    item.Status,
			Type:      item.Type,
			Amount:    item.Amount,
			AmountTwo: item.AmountTwo,
			CoinType:  item.CoinType,
			CreatedAt: item.CreatedAt,
		})
	}

	return res, nil
}

// GetStakeByUserId .
func (ub *UserBalanceRepo) GetStakeByUserId(ctx context.Context, userId int64) ([]*biz.Stake, error) {
	var stake []*Stake
	res := make([]*biz.Stake, 0)
	if err := ub.data.DB(ctx).Table("stake").Where("user_id=?", userId).Find(&stake).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, nil
		}

		return nil, errors.New(500, "PRICE CHANGE ERROR", err.Error())
	}

	for _, v := range stake {
		res = append(res, &biz.Stake{
			ID:        v.ID,
			UserId:    v.UserId,
			Status:    v.Status,
			Day:       v.Day,
			Amount:    v.Amount,
			Reward:    v.Reward,
			CreatedAt: v.CreatedAt,
			UpdatedAt: v.UpdatedAt,
		})
	}

	return res, nil
}

// InRecordNew .
func (u *UserRepo) InRecordNew(ctx context.Context, userId int64, address string, amount int64, coinType string) error {
	var err error
	var reward Reward
	reward.UserId = userId
	reward.Amount = amount
	reward.Address = address
	reward.Type = coinType // 本次分红的行为类型
	reward.Reason = "buy"  // 给我分红的理由
	err = u.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return err
	}

	return nil
}

// GetRewardFourYes .
func (ub *UserBalanceRepo) GetRewardFourYes(ctx context.Context) (*biz.Reward, error) {
	var reward *Reward
	if err := ub.data.db.Where("user_id=? and reason=?", 999999, "four_yes").Order("id desc").Table("reward").First(&reward).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		return nil, errors.New(500, "REWARD ERROR", err.Error())
	}
	return &biz.Reward{
		ID:               reward.ID,
		UserId:           reward.UserId,
		Amount:           reward.Amount,
		BalanceRecordId:  reward.BalanceRecordId,
		Type:             reward.Type,
		TypeRecordId:     reward.TypeRecordId,
		Reason:           reward.Reason,
		ReasonLocationId: reward.ReasonLocationId,
		LocationType:     reward.LocationType,
		CreatedAt:        reward.CreatedAt,
	}, nil
}

// GetUserById .
func (u *UserRepo) GetUserById(ctx context.Context, Id int64) (*biz.User, error) {
	var user User
	if err := u.data.db.Where(&User{ID: Id}).Table("user").First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("USER_NOT_FOUND", "user not found")
		}

		return nil, errors.New(500, "USER ERROR", err.Error())
	}

	return &biz.User{
		ID:                     user.ID,
		Password:               user.Password,
		Address:                user.Address,
		AddressThree:           user.AddressThree,
		AddressTwo:             user.AddressTwo,
		Amount:                 user.Amount,
		AmountBiw:              user.AmountBiw,
		Undo:                   user.Undo,
		IsDelete:               user.IsDelete,
		Out:                    user.Out,
		Lock:                   user.Lock,
		AmountUsdt:             user.AmountUsdt,
		MyTotalAmount:          user.MyTotalAmount,
		AmountUsdtGet:          user.AmountUsdtGet,
		AmountRecommendUsdtGet: user.AmountRecommendUsdtGet,
		UpdatedAt:              user.UpdatedAt,
		Last:                   user.Last,
		Vip:                    user.Vip,
	}, nil
}

// GetUserInfoByUserId .
func (ui *UserInfoRepo) GetUserInfoByUserId(ctx context.Context, userId int64) (*biz.UserInfo, error) {
	var userInfo UserInfo
	if err := ui.data.db.Where(&UserInfo{UserId: userId}).Table("user_info").First(&userInfo).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("USERINFO_NOT_FOUND", "userinfo not found")
		}

		return nil, errors.New(500, "USERINFO ERROR", err.Error())
	}

	return &biz.UserInfo{
		ID:               userInfo.ID,
		UserId:           userInfo.UserId,
		Vip:              userInfo.Vip,
		HistoryRecommend: userInfo.HistoryRecommend,
		TeamCsdBalance:   userInfo.TeamCsdBalance,
		UseVip:           userInfo.UseVip,
	}, nil
}

// GetUserByAddresses .
func (u *UserRepo) GetUserByAddresses(ctx context.Context, Addresses ...string) (map[string]*biz.User, error) {
	var users []*User
	if err := u.data.db.Table("user").Where("address IN (?)", Addresses).Find(&users).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("USER_NOT_FOUND", "user not found")
		}

		return nil, errors.New(500, "USER ERROR", err.Error())
	}

	res := make(map[string]*biz.User, 0)
	for _, item := range users {
		res[item.Address] = &biz.User{
			ID:      item.ID,
			Address: item.Address,
		}
	}
	return res, nil
}

// GetUserCount .
func (u *UserRepo) GetUserCount(ctx context.Context) (int64, error) {
	var count int64
	if err := u.data.db.Table("user").Count(&count).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return count, errors.NotFound("USER_NOT_FOUND", "user not found")
		}

		return count, errors.New(500, "USER ERROR", err.Error())
	}

	return count, nil
}

// GetUserCountToday .
func (u *UserRepo) GetUserCountToday(ctx context.Context) (int64, error) {
	var count int64
	now := time.Now().UTC()
	var startDate time.Time
	var endDate time.Time
	if 14 <= now.Hour() {
		startDate = now
		endDate = now.AddDate(0, 0, 1)
	} else {
		startDate = now.AddDate(0, 0, -1)
		endDate = now
	}
	todayStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 16, 0, 0, 0, time.UTC)
	todayEnd := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 15, 59, 59, 0, time.UTC)

	if err := u.data.db.Table("user").
		Where("created_at>=?", todayStart).Where("created_at<=?", todayEnd).Count(&count).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return count, errors.NotFound("USER_NOT_FOUND", "user not found")
		}

		return count, errors.New(500, "USER ERROR", err.Error())
	}

	return count, nil
}

// GetUserByUserIdsTwo .
func (u *UserRepo) GetUserByUserIdsTwo(ctx context.Context, userIds []int64) (map[int64]*biz.User, error) {
	var users []*User

	res := make(map[int64]*biz.User, 0)
	if err := u.data.db.Table("user").Where("id IN (?)", userIds).Find(&users).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USER_NOT_FOUND", "user not found")
		}

		return nil, errors.New(500, "USER ERROR", err.Error())
	}

	for _, item := range users {
		res[item.ID] = &biz.User{
			ID:                     item.ID,
			Address:                item.Address,
			AmountUsdtGet:          item.AmountUsdtGet,
			AmountUsdt:             item.AmountUsdt,
			AmountRecommendUsdtGet: item.AmountRecommendUsdtGet,
			MyTotalAmount:          item.MyTotalAmount,
		}
	}
	return res, nil
}

// GetUserByUserIds .
func (u *UserRepo) GetUserByUserIds(ctx context.Context, userIds ...int64) (map[int64]*biz.User, error) {
	var users []*User

	res := make(map[int64]*biz.User, 0)
	if err := u.data.db.Table("user").Where("id IN (?)", userIds).Find(&users).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USER_NOT_FOUND", "user not found")
		}

		return nil, errors.New(500, "USER ERROR", err.Error())
	}

	for _, item := range users {
		res[item.ID] = &biz.User{
			ID:      item.ID,
			Address: item.Address,
		}
	}
	return res, nil
}

// GetAllUsers .
func (u *UserRepo) GetAllUsers(ctx context.Context) ([]*biz.User, error) {
	var users []*User
	if err := u.data.db.Table("user").Order("id asc").Find(&users).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		return nil, errors.New(500, "USER ERROR", err.Error())
	}

	res := make([]*biz.User, 0)
	for _, item := range users {
		res = append(res, &biz.User{
			ID:                     item.ID,
			Address:                item.Address,
			Lock:                   item.Lock,
			AddressTwo:             item.AddressTwo,
			AddressThree:           item.AddressThree,
			Amount:                 item.Amount,
			AmountUsdt:             item.AmountUsdt,
			AmountUsdtGet:          item.AmountUsdtGet,
			MyTotalAmount:          item.MyTotalAmount,
			AmountRecommendUsdtGet: item.AmountRecommendUsdtGet,
			RecommendLevel:         item.RecommendLevel,
			Last:                   item.Last,
			Vip:                    item.Vip,
			LockReward:             item.LockReward,
		})
	}
	return res, nil
}

// GetUsers .
func (u *UserRepo) GetUsers(ctx context.Context, b *biz.Pagination, address string) ([]*biz.User, error, int64) {
	var (
		users []*User
		count int64
	)
	instance := u.data.db.Table("user")

	if "" != address {
		instance = instance.Where("address=?", address)
	}

	instance = instance.Count(&count)
	if err := instance.Scopes(Paginate(b.PageNum, b.PageSize)).Order("id desc").Find(&users).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("USER_NOT_FOUND", "user not found"), 0
		}

		return nil, errors.New(500, "USER ERROR", err.Error()), 0
	}

	res := make([]*biz.User, 0)
	for _, item := range users {
		res = append(res, &biz.User{
			ID:        item.ID,
			Address:   item.Address,
			CreatedAt: item.CreatedAt,
		})
	}
	return res, nil, count
}

// CreateUser .
func (u *UserRepo) CreateUser(ctx context.Context, uc *biz.User) (*biz.User, error) {
	var user User
	user.Address = uc.Address
	user.Password = uc.Password

	//user.AddressTwo = uc.AddressTwo
	//user.PrivateKey = uc.PrivateKey
	//user.AddressThree = uc.AddressThree
	//user.PrivateKeyThree = uc.PrivateKeyThree
	//user.WordThree = uc.WordThree

	res := u.data.DB(ctx).Table("user").Create(&user)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_USER_ERROR", "用户创建失败")
	}

	return &biz.User{
		ID:       user.ID,
		Address:  user.Address,
		Password: user.Password,
	}, nil
}

// CreateUserInfo .
func (ui *UserInfoRepo) CreateUserInfo(ctx context.Context, u *biz.User) (*biz.UserInfo, error) {
	var userInfo UserInfo
	userInfo.UserId = u.ID

	res := ui.data.DB(ctx).Table("user_info").Create(&userInfo)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_USER_INFO_ERROR", "用户信息创建失败")
	}

	return &biz.UserInfo{
		ID:               userInfo.ID,
		UserId:           userInfo.UserId,
		Vip:              0,
		HistoryRecommend: 0,
	}, nil
}

// UpdateUserInfo .
func (ui *UserInfoRepo) UpdateUserInfo(ctx context.Context, u *biz.UserInfo) (*biz.UserInfo, error) {
	var userInfo UserInfo
	userInfo.Vip = u.Vip
	userInfo.HistoryRecommend = u.HistoryRecommend

	res := ui.data.DB(ctx).Table("user_info").Where("user_id=?", u.UserId).Updates(&userInfo)
	if res.Error != nil {
		return nil, errors.New(500, "UPDATE_USER_INFO_ERROR", "用户信息修改失败")
	}

	return &biz.UserInfo{
		ID:               userInfo.ID,
		UserId:           userInfo.UserId,
		Vip:              userInfo.Vip,
		HistoryRecommend: userInfo.HistoryRecommend,
	}, nil
}

// GetUserRecommendsFour .
func (ur *UserRecommendRepo) GetUserRecommendsFour(ctx context.Context) ([]*biz.UserRecommend, error) {
	var userRecommends []*UserRecommend
	res := make([]*biz.UserRecommend, 0)
	if err := ur.data.db.Table("user_recommend").Order("total desc").Limit(4).Find(&userRecommends).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USER_RECOMMEND_NOT_FOUND", "user recommend not found")
		}

		return nil, errors.New(500, "USER RECOMMEND ERROR", err.Error())
	}

	for _, userRecommend := range userRecommends {
		res = append(res, &biz.UserRecommend{
			UserId:        userRecommend.UserId,
			RecommendCode: userRecommend.RecommendCode,
			Total:         userRecommend.Total,
		})
	}

	return res, nil
}

// RecommendLocationRewardBiw .
func (ub *UserBalanceRepo) RecommendLocationRewardBiw(ctx context.Context, userId int64, rewardAmount int64, recommendNum int64, stop string, tmpMaxNew int64, feeRate int64) (int64, error) {
	var err error
	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", rewardAmount), "recommend_total": gorm.Expr("recommend_location_total + ?", rewardAmount)}).Error; nil != err {
		return 0, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	//if "stop" == stop {
	//	if 0 < userBalance.BalanceDhb {
	//		tmp := tmpMaxNew
	//		tmp -= tmp * feeRate / 1000
	//		if err = ub.data.DB(ctx).Table("user_balance").
	//			Where("user_id=?", userId).
	//			Updates(map[string]interface{}{"balance_dhb": 0, "balance_usdt": gorm.Expr("balance_usdt + ?", tmp)}).Error; nil != err {
	//			return 0, errors.NotFound("user balance err", "user balance not found")
	//		}
	//
	//		var userBalanceRecode UserBalanceRecord
	//		userBalanceRecode.Balance = userBalance.BalanceDhb
	//		userBalanceRecode.UserId = userBalance.UserId
	//		userBalanceRecode.Type = "exchange"
	//		userBalanceRecode.CoinType = "dhb"
	//		userBalanceRecode.Amount = tmp
	//		err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	//		if err != nil {
	//			return 0, nil
	//		}
	//
	//		var (
	//			reward Reward
	//		)
	//
	//		reward.UserId = userId
	//		reward.Amount = userBalance.BalanceDhb
	//		reward.AmountB = tmp
	//		reward.Type = "exchange_system" // 本次分红的行为类型
	//		reward.TypeRecordId = userBalanceRecode.ID
	//		reward.Reason = "exchange_2" // 给我分红的理由
	//		err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	//		if err != nil {
	//			return 0, nil
	//		}
	//	}
	//}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "reward"
	userBalanceRecode.CoinType = "usdt"
	userBalanceRecode.Amount = rewardAmount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	var reward Reward
	reward.UserId = userBalance.UserId
	reward.Amount = rewardAmount
	reward.BalanceRecordId = userBalanceRecode.ID
	reward.Type = "system_reward_recommend_location" // 本次分红的行为类型
	reward.Reason = "recommend_location"             // 给我分红的理由
	reward.ReasonLocationId = recommendNum
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// GetUserRecommends .
func (ur *UserRecommendRepo) GetUserRecommends(ctx context.Context) ([]*biz.UserRecommend, error) {
	var userRecommends []*UserRecommend
	res := make([]*biz.UserRecommend, 0)
	if err := ur.data.db.Table("user_recommend").Find(&userRecommends).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USER_RECOMMEND_NOT_FOUND", "user recommend not found")
		}

		return nil, errors.New(500, "USER RECOMMEND ERROR", err.Error())
	}

	for _, userRecommend := range userRecommends {
		res = append(res, &biz.UserRecommend{
			UserId:        userRecommend.UserId,
			RecommendCode: userRecommend.RecommendCode,
			Total:         userRecommend.Total,
		})
	}

	return res, nil
}

// GetUserRecommendByUserId .
func (ur *UserRecommendRepo) GetUserRecommendByUserId(ctx context.Context, userId int64) (*biz.UserRecommend, error) {
	var userRecommend UserRecommend
	if err := ur.data.db.Where(&UserRecommend{UserId: userId}).Table("user_recommend").First(&userRecommend).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("USER_RECOMMEND_NOT_FOUND", "user recommend not found")
		}

		return nil, errors.New(500, "USER RECOMMEND ERROR", err.Error())
	}

	return &biz.UserRecommend{
		UserId:        userRecommend.UserId,
		RecommendCode: userRecommend.RecommendCode,
	}, nil
}

// GetUserRecommendByCode .
func (ur *UserRecommendRepo) GetUserRecommendByCode(ctx context.Context, code string) ([]*biz.UserRecommend, error) {
	var (
		userRecommends []*UserRecommend
	)
	res := make([]*biz.UserRecommend, 0)

	instance := ur.data.db.Table("user_recommend").Where("recommend_code=?", code)

	if err := instance.Find(&userRecommends).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USER_RECOMMEND_NOT_FOUND", "user recommend not found")
		}

		return nil, errors.New(500, "USER RECOMMEND ERROR", err.Error())
	}

	for _, userRecommend := range userRecommends {
		res = append(res, &biz.UserRecommend{
			UserId:        userRecommend.UserId,
			RecommendCode: userRecommend.RecommendCode,
			CreatedAt:     userRecommend.CreatedAt,
		})
	}

	return res, nil
}

// GetUserRecommendLikeCodeSum .
func (ur *UserRecommendRepo) GetUserRecommendLikeCodeSum(ctx context.Context, code string) (int64, error) {
	var total UserBalanceTotal
	if err := ur.data.db.Where("recommend_code Like ?", code+"%").Table("user_recommend").Select("sum(id) as total").Take(&total).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, errors.NotFound("USER_BALANCE_RECORD_NOT_FOUND", "user balance not found")
		}
	}

	return total.Total, nil
}

// GetUserRecommendLikeCode .
func (ur *UserRecommendRepo) GetUserRecommendLikeCode(ctx context.Context, code string) ([]*biz.UserRecommend, error) {
	var userRecommends []*UserRecommend
	res := make([]*biz.UserRecommend, 0)
	if err := ur.data.db.Where("recommend_code Like ?", code+"%").Table("user_recommend").Find(&userRecommends).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USER_RECOMMEND_NOT_FOUND", "user recommend not found")
		}

		return nil, errors.New(500, "USER RECOMMEND ERROR", err.Error())
	}

	for _, userRecommend := range userRecommends {
		res = append(res, &biz.UserRecommend{
			UserId:        userRecommend.UserId,
			RecommendCode: userRecommend.RecommendCode,
		})
	}

	return res, nil
}

// CreateUserRecommend .
func (ur *UserRecommendRepo) CreateUserRecommend(ctx context.Context, u *biz.User, recommendUser *biz.UserRecommend) (*biz.UserRecommend, error) {
	var tmpRecommendCode string
	if nil != recommendUser && 0 < recommendUser.UserId {
		tmpRecommendCode = "D" + strconv.FormatInt(recommendUser.UserId, 10)
		if "" != recommendUser.RecommendCode {
			tmpRecommendCode = recommendUser.RecommendCode + tmpRecommendCode
		}
	}

	var userRecommend UserRecommend
	userRecommend.UserId = u.ID
	userRecommend.RecommendCode = tmpRecommendCode

	res := ur.data.DB(ctx).Table("user_recommend").Create(&userRecommend)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_USER_RECOMMEND_ERROR", "用户推荐关系创建失败")
	}

	return &biz.UserRecommend{
		ID:            userRecommend.ID,
		UserId:        userRecommend.UserId,
		RecommendCode: userRecommend.RecommendCode,
	}, nil
}

// CreateUserRecommendArea .
func (ur *UserRecommendRepo) CreateUserRecommendArea(ctx context.Context, u *biz.User, recommendUser *biz.UserRecommend) (bool, error) {
	var tmpRecommendCode string
	if nil != recommendUser && 0 < recommendUser.UserId {
		tmpRecommendCode = "D" + strconv.FormatInt(recommendUser.UserId, 10)
		if "" != recommendUser.RecommendCode {
			tmpRecommendCode = recommendUser.RecommendCode + tmpRecommendCode
		}
	}

	var myUserRecommendArea UserRecommendArea
	if err := ur.data.db.Where("recommend_code=?", tmpRecommendCode).Table("user_recommend_area").First(&myUserRecommendArea).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return false, errors.New(500, "USER RECOMMEND ERROR", err.Error())
		}

		// 业务上限制了错误的上一级未insert下一级优先insert的情况
		var userRecommendArea UserRecommendArea
		userRecommendArea.RecommendCode = tmpRecommendCode + "D" + strconv.FormatInt(u.ID, 10)
		userRecommendArea.Num = myUserRecommendArea.Num + int64(len(strings.Split(userRecommendArea.RecommendCode, "D"))-1)
		res := ur.data.DB(ctx).Table("user_recommend_area").Create(&userRecommendArea)
		if res.Error != nil {
			return false, errors.New(500, "CREATE_USER_RECOMMEND_AREA_ERROR", "用户推荐关系链路创建失败")
		}

	} else {
		res := ur.data.DB(ctx).Table("user_recommend_area").
			Where("id=? and version=?", myUserRecommendArea.ID, myUserRecommendArea.Version).
			Updates(map[string]interface{}{"version": gorm.Expr("version + ?", 1), "num": gorm.Expr("num + ?", 1), "recommend_code": tmpRecommendCode + "D" + strconv.FormatInt(u.ID, 10)})
		if 0 == res.RowsAffected || nil != res.Error {
			return false, errors.New(500, "CREATE_USER_RECOMMEND_AREA_ERROR", "用户推荐关系链路修改失败")
		}
	}

	return true, nil
}

// CreateUserArea .
func (ur *UserRecommendRepo) CreateUserArea(ctx context.Context, u *biz.User) (bool, error) {
	// 业务上限制了错误的上一级未insert下一级优先insert的情况
	var userArea UserArea
	userArea.UserId = u.ID
	res := ur.data.DB(ctx).Table("user_area").Create(&userArea)
	if res.Error != nil {
		return false, errors.New(500, "CREATE_USER_AREA_ERROR", "用户区信息创建失败")
	}

	return true, nil
}

// DeleteOrOriginUserRecommendArea .
func (ur *UserRecommendRepo) DeleteOrOriginUserRecommendArea(ctx context.Context, code string, originCode string) (bool, error) {
	//var myUserRecommendArea []*UserRecommendArea
	//if err := ur.data.db.Where("recommend_code like ? and status=?", originCode+"%", 0).Table("user_recommend_area").Find(&myUserRecommendArea).Error; err != nil {
	//	if !errors.Is(err, gorm.ErrRecordNotFound) {
	//		return false, errors.New(500, "USER RECOMMEND ERROR", err.Error())
	//	}
	//
	//	res := ur.data.DB(ctx).Table("user_recommend_area").
	//		Where("recommend_code=? and status=?", code, 0).
	//		Updates(map[string]interface{}{"recommend_code": originCode})
	//	if 0 == res.RowsAffected || nil != res.Error {
	//		return false, errors.New(500, "UPDATE_USER_RECOMMEND_AREA_ERROR", "用户推荐关系链路修改失败")
	//	}
	//	return true, nil
	//}
	//
	//if 2 > len(myUserRecommendArea) {
	res := ur.data.DB(ctx).Table("user_recommend_area").
		Where("recommend_code=?", code).
		Updates(map[string]interface{}{"recommend_code": originCode, "num": gorm.Expr("num - ?", 1)})
	if 0 == res.RowsAffected || nil != res.Error {
		return false, errors.New(500, "UPDATE_USER_RECOMMEND_AREA_ERROR", "用户推荐关系链路修改失败")
	}
	//} else {
	//	res := ur.data.DB(ctx).Table("user_recommend_area").
	//		Where("recommend_code=? and status=?", code, 0).
	//		Updates(map[string]interface{}{"status": 1})
	//	if 0 == res.RowsAffected || nil != res.Error {
	//		return false, errors.New(500, "UPDATE_USER_RECOMMEND_AREA_ERROR", "用户推荐关系链路修改失败")
	//	}
	//}

	return true, nil
}

// GetUserRecommendLowArea .
func (ur *UserRecommendRepo) GetUserRecommendLowArea(ctx context.Context, code string) ([]*biz.UserRecommendArea, error) {

	var firstRecommendArea *UserRecommendArea
	if err := ur.data.db.Order("num desc").Table("user_recommend_area").First(&firstRecommendArea).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New(500, "USER RECOMMEND NOT FOUND", err.Error())
		}

		return nil, errors.New(500, "USER RECOMMEND ERROR", err.Error())
	}

	var myUserRecommendAreas []*UserRecommendArea
	if err := ur.data.db.Where("recommend_code like ?", code+"%").Table("user_recommend_area").Find(&myUserRecommendAreas).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New(500, "USER RECOMMEND NOT FOUND", err.Error())
		}

		return nil, errors.New(500, "USER RECOMMEND ERROR", err.Error())
	}

	res := make([]*biz.UserRecommendArea, 0)
	for _, v := range myUserRecommendAreas {
		if firstRecommendArea.ID == v.ID {
			continue
		}

		res = append(res, &biz.UserRecommendArea{
			ID:            v.ID,
			RecommendCode: v.RecommendCode,
			Num:           v.Num,
		})
	}

	return res, nil
}

// UpdateUserRecommendTotal .
func (ur *UserRecommendRepo) UpdateUserRecommendTotal(ctx context.Context, userId int64, total int64) error {
	// 业务上限制了错误的上一级未insert下一级优先insert的情况
	var err error
	if err = ur.data.DB(ctx).Table("user_recommend").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"total": gorm.Expr("total + ?", total)}).Error; nil != err {
		return err
	}

	return nil
}

// GetUserArea .
func (ur *UserRecommendRepo) GetUserArea(ctx context.Context, userId int64) (*biz.UserArea, error) {

	var userArea *UserArea
	if err := ur.data.db.Where("user_id=?", userId).Table("user_area").First(&userArea).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New(500, "USER AREA NOT FOUND", err.Error())
		}

		return nil, errors.New(500, "USER AREA ERROR", err.Error())
	}

	return &biz.UserArea{
		ID:         userArea.ID,
		UserId:     userArea.UserId,
		Amount:     userArea.Amount,
		SelfAmount: userArea.SelfAmount,
		Level:      userArea.Level,
	}, nil
}

// GetUserAreas .
func (ur *UserRecommendRepo) GetUserAreas(ctx context.Context, userIds []int64) ([]*biz.UserArea, error) {

	var userAreas []*UserArea
	if err := ur.data.db.Where("user_id in (?)", userIds).Table("user_area").Find(&userAreas).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New(500, "USER AREA NOT FOUND", err.Error())
		}

		return nil, errors.New(500, "USER AREA ERROR", err.Error())
	}

	res := make([]*biz.UserArea, 0)
	for _, v := range userAreas {
		res = append(res, &biz.UserArea{
			ID:         v.ID,
			UserId:     v.UserId,
			Amount:     v.Amount,
			SelfAmount: v.SelfAmount,
			Level:      v.Level,
		})
	}

	return res, nil
}

// UpdateUserRecommend .
func (ur *UserRecommendRepo) UpdateUserRecommend(ctx context.Context, u *biz.User, recommendUser *biz.UserRecommend) (bool, error) {
	var tmpRecommendCode string
	if nil != recommendUser && 0 < recommendUser.UserId {
		tmpRecommendCode = "D" + strconv.FormatInt(recommendUser.UserId, 10)
		if "" != recommendUser.RecommendCode {
			tmpRecommendCode = recommendUser.RecommendCode + tmpRecommendCode
		}
	}

	var userRecommend UserRecommend
	userRecommend.RecommendCode = tmpRecommendCode

	res := ur.data.DB(ctx).Table("user_recommend").Where("user_id", u.ID).Updates(&userRecommend)
	if res.Error != nil {
		return false, errors.New(500, "CREATE_USER_RECOMMEND_ERROR", "用户推荐关系修改失败")
	}

	return true, nil
}

// CreateUserBalance .
func (ub UserBalanceRepo) CreateUserBalance(ctx context.Context, u *biz.User) (*biz.UserBalance, error) {
	var userBalance UserBalance
	userBalance.UserId = u.ID
	res := ub.data.DB(ctx).Table("user_balance").Create(&userBalance)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_USER_BALANCE_ERROR", "用户余额信息创建失败")
	}

	return &biz.UserBalance{
		ID:          userBalance.ID,
		UserId:      userBalance.UserId,
		BalanceUsdt: userBalance.BalanceUsdt,
		BalanceDhb:  userBalance.BalanceDhb,
	}, nil
}

// CreateUserBalanceLock .
func (ub UserBalanceRepo) CreateUserBalanceLock(ctx context.Context, u *biz.User) (*biz.UserBalance, error) {
	var userBalance UserBalance
	userBalance.UserId = u.ID
	res := ub.data.DB(ctx).Table("user_balance_lock").Create(&userBalance)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_USER_BALANCE_ERROR", "用户余额信息创建失败")
	}

	return &biz.UserBalance{
		ID:          userBalance.ID,
		UserId:      userBalance.UserId,
		BalanceUsdt: userBalance.BalanceUsdt,
		BalanceDhb:  userBalance.BalanceDhb,
	}, nil
}

// GetUserBalance .
func (ub UserBalanceRepo) GetUserBalance(ctx context.Context, userId int64) (*biz.UserBalance, error) {
	var userBalance UserBalance
	if err := ub.data.db.Where("user_id=?", userId).Table("user_balance").First(&userBalance).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("USER_BALANCE_NOT_FOUND", "user balance not found")
		}

		return nil, errors.New(500, "USER BALANCE ERROR", err.Error())
	}

	return &biz.UserBalance{
		ID:                  userBalance.ID,
		UserId:              userBalance.UserId,
		BalanceUsdt:         userBalance.BalanceUsdt,
		BalanceUsdt2:        userBalance.BalanceUsdtNew,
		BalanceDhb:          userBalance.BalanceDhb,
		RecommendTotal:      userBalance.RecommendTotal,
		AreaTotal:           userBalance.AreaTotal,
		LocationTotal:       userBalance.LocationTotal,
		FourTotal:           userBalance.FourTotal,
		BalanceC:            userBalance.BalanceC,
		AreaTotalFloat:      userBalance.AreaTotalFloat,
		AreaTotalFloatTwo:   userBalance.AreaTotalFloatTwo,
		RecommendTotalFloat: userBalance.RecommendTotalFloat,
		LocationTotalFloat:  userBalance.LocationTotalFloat,
		BalanceRawFloat:     userBalance.BalanceRawFloat,
		BalanceUsdtFloat:    userBalance.BalanceUsdtFloat,
		BalanceKsdtFloat:    userBalance.BalanceKsdtFloat,
		AreaTotalFloatThree: userBalance.AreaTotalFloatThree,
	}, nil
}

// GetUserBalanceLock .
func (ub UserBalanceRepo) GetUserBalanceLock(ctx context.Context, userId int64) (*biz.UserBalance, error) {
	var userBalance UserBalance
	if err := ub.data.db.Where("user_id=?", userId).Table("user_balance_lock").First(&userBalance).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("USER_BALANCE_NOT_FOUND", "user balance not found")
		}

		return nil, errors.New(500, "USER BALANCE ERROR", err.Error())
	}

	return &biz.UserBalance{
		ID:          userBalance.ID,
		UserId:      userBalance.UserId,
		BalanceUsdt: userBalance.BalanceUsdt,
		BalanceDhb:  userBalance.BalanceDhb,
	}, nil
}

// Trade .
func (ub *UserBalanceRepo) Trade(ctx context.Context, userId int64, amount int64, amountB int64, amountRel int64, amountBRel int64, tmpRecommendUserIdsInt []int64, amount2 int64) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance_lock").
		Where("user_id=? and balance_usdt>=?", userId, amount).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_dhb>=?", userId, amountB-amountBRel).
		Updates(map[string]interface{}{"balance_dhb": gorm.Expr("balance_dhb - ?", amountB-amountBRel)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "trade"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	var userBalanceRecode1 UserBalanceRecord
	userBalanceRecode1.Balance = userBalance.BalanceDhb
	userBalanceRecode1.UserId = userBalance.UserId
	userBalanceRecode1.Type = "trade_dhb"
	userBalanceRecode1.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode1).Error
	if err != nil {
		return err
	}

	var trade Trade
	trade.AmountCsd = amount
	trade.AmountHbs = amountB
	trade.RelAmountCsd = amountRel
	trade.RelAmountHbs = amountBRel
	trade.UserId = userId
	trade.Status = "default"
	err = ub.data.DB(ctx).Table("trade").Create(&trade).Error
	if err != nil {
		return err
	}

	if 0 < len(tmpRecommendUserIdsInt) {
		if err = ub.data.DB(ctx).Table("user_info").
			Where("user_id in (?)", tmpRecommendUserIdsInt).
			Updates(map[string]interface{}{"team_csd_balance": gorm.Expr("team_csd_balance - ?", amount-amountRel)}).Error; nil != err {
			return errors.NotFound("user balance err", "user balance not found")
		}
	}

	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", amountRel)}).Error; nil != err {
		return errors.NotFound("user balance err", "user balance not found")
	}

	return nil
}

// LocationReward .
func (ub *UserBalanceRepo) LocationReward(ctx context.Context, userId int64, amount int64, locationId int64, myLocationId int64, locationType string) (int64, error) {
	var err error
	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", amount)}).Error; nil != err {
		return 0, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "reward"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	var reward Reward
	reward.UserId = userBalance.UserId
	reward.Amount = amount
	reward.BalanceRecordId = userBalanceRecode.ID
	reward.Type = "location" // 本次分红的行为类型
	reward.TypeRecordId = locationId
	reward.Reason = "location" // 给我分红的理由
	reward.ReasonLocationId = myLocationId
	reward.LocationType = locationType
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// WithdrawReward .
func (ub *UserBalanceRepo) WithdrawReward(ctx context.Context, userId int64, amount int64, locationId int64, myLocationId int64, locationType string) (int64, error) {
	var err error
	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", amount)}).Error; nil != err {
		return 0, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "reward"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	var reward Reward
	reward.UserId = userBalance.UserId
	reward.Amount = amount
	reward.BalanceRecordId = userBalanceRecode.ID
	reward.Type = "withdraw" // 本次分红的行为类型
	reward.TypeRecordId = locationId
	reward.Reason = "location" // 给我分红的理由
	reward.ReasonLocationId = myLocationId
	reward.LocationType = locationType
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// Deposit .
func (ub *UserBalanceRepo) Deposit(ctx context.Context, userId int64, amount int64) (int64, error) {
	var err error
	//if err = ub.data.DB(ctx).Table("user_balance").
	//	Where("user_id=?", userId).
	//	Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", amount)}).Error; nil != err {
	//	return 0, errors.NotFound("user balance err", "user balance not found")
	//}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "deposit"
	userBalanceRecode.CoinType = "usdt"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// DepositLast .
func (ub *UserBalanceRepo) DepositLast(ctx context.Context, userId int64, lastAmount int64, locationId int64) (int64, error) {
	var (
		err error
	)
	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", lastAmount)}).Error; nil != err {
		return 0, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "reward"
	userBalanceRecode.Amount = lastAmount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	var reward Reward
	reward.UserId = userBalance.UserId
	reward.Amount = lastAmount
	reward.BalanceRecordId = userBalanceRecode.ID
	reward.Type = "last"          // 本次分红的行为类型
	reward.Reason = "last_reward" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return 0, err
	}

	res := ub.data.db.Table("location").
		Where("id=?", locationId).
		Updates(map[string]interface{}{"stop_location_again": "1"})
	if 0 == res.RowsAffected || res.Error != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// DepositDhb .
func (ub *UserBalanceRepo) DepositDhb(ctx context.Context, userId int64, amount int64) (int64, error) {
	var err error
	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_dhb": gorm.Expr("balance_dhb + ?", amount)}).Error; nil != err {
		return 0, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceDhb
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "deposit"
	userBalanceRecode.CoinType = "dhb"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// UpdateWithdrawAmount .
func (ub *UserBalanceRepo) UpdateWithdrawAmount(ctx context.Context, id int64, status string, amount int64) (*biz.Withdraw, error) {
	var withdraw Withdraw
	withdraw.Status = status
	withdraw.Amount = amount
	res := ub.data.DB(ctx).Table("withdraw").Where("id=?", id).Updates(&withdraw)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_WITHDRAW_ERROR", "提现记录修改失败")
	}

	return &biz.Withdraw{
		ID:              withdraw.ID,
		UserId:          withdraw.UserId,
		Amount:          withdraw.Amount,
		RelAmount:       withdraw.RelAmount,
		BalanceRecordId: withdraw.BalanceRecordId,
		Status:          withdraw.Status,
		Type:            withdraw.Type,
		CreatedAt:       withdraw.CreatedAt,
	}, nil
}

// ToAddressAmountUsdt .
func (ub *UserBalanceRepo) ToAddressAmountUsdt(ctx context.Context, userId int64, toUserId int64, amount float64, address string) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_usdt_float>=?", userId, amount).
		Updates(map[string]interface{}{"balance_usdt_float": gorm.Expr("balance_usdt_float - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	if res2 := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", toUserId).
		Updates(map[string]interface{}{"balance_usdt_float": gorm.Expr("balance_usdt_float + ?", amount)}); 0 == res2.RowsAffected || nil != res2.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "to_amount"
	userBalanceRecode.CoinType = "USDT"
	userBalanceRecode.AmountNew = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	var (
		reward Reward
	)

	reward.UserId = userId
	reward.AmountNew = amount
	reward.Type = "USDT" // 本次分红的行为类型
	reward.TypeRecordId = toUserId
	reward.Reason = "to_amount" // 给我分红的理由
	reward.Address = address
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return err
	}

	return nil
}

// ToAddressAmountKsdt .
func (ub *UserBalanceRepo) ToAddressAmountKsdt(ctx context.Context, userId int64, toUserId int64, amount float64, address string) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_ksdt_float>=?", userId, amount).
		Updates(map[string]interface{}{"balance_ksdt_float": gorm.Expr("balance_ksdt_float - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	if res2 := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", toUserId).
		Updates(map[string]interface{}{"balance_ksdt_float": gorm.Expr("balance_ksdt_float + ?", amount)}); 0 == res2.RowsAffected || nil != res2.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "to_amount"
	userBalanceRecode.CoinType = "KSDT"
	userBalanceRecode.AmountNew = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	var (
		reward Reward
	)

	reward.UserId = userId
	reward.AmountNew = amount
	reward.Type = "KSDT" // 本次分红的行为类型
	reward.TypeRecordId = toUserId
	reward.Reason = "to_amount" // 给我分红的理由
	reward.Address = address
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return err
	}

	return nil
}

// UnStakeAmount .
func (ub *UserBalanceRepo) UnStakeAmount(ctx context.Context, userId int64, stakeId int64, amount float64) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_raw_float>=?", userId, amount).
		Updates(map[string]interface{}{"balance_raw_float": gorm.Expr("balance_raw_float + ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	if err = ub.data.DB(ctx).Table("stake").
		Where("id=? and status=?", stakeId, 0).
		Updates(map[string]interface{}{"status": 2}).Error; nil != err {
		return errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "un_stake"
	userBalanceRecode.CoinType = "RAW"
	userBalanceRecode.AmountNew = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	var (
		reward Reward
	)

	reward.UserId = userId
	reward.AmountNew = amount
	reward.Type = "RAW" // 本次分红的行为类型
	reward.TypeRecordId = stakeId
	reward.Reason = "un_stake" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return err
	}

	return nil
}

// StakeAmount .
func (ub *UserBalanceRepo) StakeAmount(ctx context.Context, userId int64, amount float64, day int64) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_raw_float>=?", userId, amount).
		Updates(map[string]interface{}{"balance_raw_float": gorm.Expr("balance_raw_float - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "stake"
	userBalanceRecode.CoinType = "RAW"
	userBalanceRecode.AmountNew = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	var (
		reward Reward
	)

	reward.UserId = userId
	reward.AmountNew = amount
	reward.Type = "RAW" // 本次分红的行为类型
	reward.TypeRecordId = int64(day)
	reward.Reason = "stake" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return err
	}

	var (
		stake Stake
	)
	stake.Amount = amount
	stake.UserId = userId
	stake.Day = day
	err = ub.data.DB(ctx).Table("stake").Create(&stake).Error
	if err != nil {
		return err
	}

	return nil
}

// ToAddressAmountRaw .
func (ub *UserBalanceRepo) ToAddressAmountRaw(ctx context.Context, userId int64, toUserId int64, amount float64, address string) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_raw_float>=?", userId, amount).
		Updates(map[string]interface{}{"balance_raw_float": gorm.Expr("balance_raw_float - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	if res2 := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", toUserId).
		Updates(map[string]interface{}{"balance_raw_float": gorm.Expr("balance_raw_float + ?", amount)}); 0 == res2.RowsAffected || nil != res2.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "to_amount"
	userBalanceRecode.CoinType = "RAW"
	userBalanceRecode.AmountNew = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	var (
		reward Reward
	)

	reward.UserId = userId
	reward.AmountNew = amount
	reward.Type = "RAW" // 本次分红的行为类型
	reward.TypeRecordId = toUserId
	reward.Reason = "to_amount" // 给我分红的理由
	reward.Address = address
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return err
	}

	return nil
}

// WithdrawUsdt2 .
func (ub *UserBalanceRepo) WithdrawUsdt2(ctx context.Context, userId int64, amount float64) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_usdt_float>=?", userId, amount).
		Updates(map[string]interface{}{"balance_usdt_float": gorm.Expr("balance_usdt_float - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "withdraw"
	userBalanceRecode.CoinType = "USDT"
	userBalanceRecode.AmountNew = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	return nil
}

// GetRewardBuyYes .
func (ub *UserBalanceRepo) GetRewardBuyYes(ctx context.Context) ([]*biz.Reward, error) {
	var rewards []*Reward

	now := time.Now().UTC()
	var startDate time.Time
	var endDate time.Time
	if 16 <= now.Hour() {
		startDate = now
		endDate = now.AddDate(0, 0, 1)
	} else {
		startDate = now.AddDate(0, 0, -1)
		endDate = now
	}
	todayStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 16, 0, 0, 0, time.UTC)
	todayEnd := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 16, 0, 0, 0, time.UTC)

	res := make([]*biz.Reward, 0)
	if err := ub.data.db.
		Where("created_at>=?", todayStart).
		Where("created_at<?", todayEnd).
		Where("reason=?", "buy").
		Table("reward").Find(&rewards).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, nil
		}

		return nil, errors.New(500, "REWARD ERROR", err.Error())
	}

	for _, reward := range rewards {
		res = append(res, &biz.Reward{
			ID:               reward.ID,
			UserId:           reward.UserId,
			Amount:           reward.Amount,
			BalanceRecordId:  reward.BalanceRecordId,
			Type:             reward.Type,
			TypeRecordId:     reward.TypeRecordId,
			Reason:           reward.Reason,
			ReasonLocationId: reward.ReasonLocationId,
			LocationType:     reward.LocationType,
			AmountNew:        reward.AmountNew,
		})
	}

	return res, nil
}

// GetSystemYesterdayLocationReward .
func (ub *UserBalanceRepo) GetSystemYesterdayLocationReward(ctx context.Context) ([]*biz.UserBalanceRecord, error) {
	var rewards []*UserBalanceRecord

	now := time.Now().UTC()
	var startDate time.Time
	var endDate time.Time
	if 16 <= now.Hour() {
		startDate = now
		endDate = now.AddDate(0, 0, 1)
	} else {
		startDate = now.AddDate(0, 0, -1)
		endDate = now
	}
	todayStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 16, 0, 0, 0, time.UTC)
	todayEnd := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 16, 0, 0, 0, time.UTC)

	if err := ub.data.db.
		Where("created_at>=?", todayStart).
		Where("created_at<?", todayEnd).
		Where("type=?", "exchange").
		Table("user_balance_record").Find(&rewards).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("REWARD_NOT_FOUND", "reward not found")
		}

		return nil, errors.New(500, "REWARD ERROR", err.Error())
	}

	res := make([]*biz.UserBalanceRecord, 0)
	for _, reward := range rewards {
		res = append(res, &biz.UserBalanceRecord{
			ID:           reward.ID,
			UserId:       reward.UserId,
			Balance:      reward.Balance,
			Amount:       reward.Amount,
			Type:         reward.Type,
			CoinType:     reward.CoinType,
			CreatedAt:    reward.CreatedAt,
			UpdatedAt:    reward.UpdatedAt,
			AmountNew:    reward.AmountNew,
			AmountNewTwo: reward.AmountNewTwo,
		})
	}

	return res, nil
}

// Exchange .
func (ub *UserBalanceRepo) Exchange(ctx context.Context, userId int64, amountUsdt, fee, amountRawSub float64) error {
	var (
		err error
	)

	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_usdt_float>=?", userId, amountUsdt).
		Updates(map[string]interface{}{"balance_usdt_float": gorm.Expr("balance_usdt_float - ?", amountUsdt), "balance_raw_float": gorm.Expr("balance_raw_float + ?", amountRawSub)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	res := ub.data.DB(ctx).Table("total").Where("id=?", 1).
		Updates(map[string]interface{}{"two": gorm.Expr("two + ?", fee), "three": gorm.Expr("three + ?", amountRawSub+fee)})
	if res.Error != nil {
		return errors.New(500, "UPDATE_USER_ERROR", "one信息修改失败")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.BalanceNew = userBalance.BalanceUsdtFloat
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "exchange"
	userBalanceRecode.CoinType = "RAW"
	userBalanceRecode.AmountNew = amountRawSub
	userBalanceRecode.AmountNewTwo = fee
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	var (
		reward Reward
	)

	reward.UserId = userId
	reward.AmountNew = amountRawSub
	reward.AmountNewTwo = amountUsdt
	reward.Type = "exchange" // 本次分红的行为类型
	reward.TypeRecordId = userBalanceRecode.ID
	reward.Reason = "exchange" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return err
	}

	return nil
}

// Exchange2 .
func (ub *UserBalanceRepo) Exchange2(ctx context.Context, userId int64, amount int64, amountUsdtSub int64, amountUsdt int64, locationId int64) error {
	var (
		err error
	)
	//res := ub.data.DB(ctx).Table("location_new").
	//	Where("id=?", locationId).
	//	Where("status=?", "running").
	//	Updates(map[string]interface{}{"current_max_new": gorm.Expr("current_max_new + ?", amountUsdt)})
	//if 0 == res.RowsAffected || res.Error != nil {
	//	return res.Error
	//}

	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_dhb>=?", userId, amount).
		Updates(map[string]interface{}{"balance_dhb": gorm.Expr("balance_dhb - ?", amount), "balance_c": gorm.Expr("balance_c + ?", amountUsdtSub)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = amount
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "exchange_c"
	userBalanceRecode.CoinType = "dhb"
	userBalanceRecode.Amount = amountUsdtSub
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	var (
		reward Reward
	)

	reward.UserId = userId
	reward.Amount = amount
	reward.AmountB = amountUsdt
	reward.Type = "exchange_c" // 本次分红的行为类型
	reward.TypeRecordId = userBalanceRecode.ID
	reward.Reason = "exchange_c" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return err
	}

	return nil
}

// WithdrawUsdt3 .
func (ub *UserBalanceRepo) WithdrawUsdt3(ctx context.Context, userId int64, amount int64) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_usdt_new>=?", userId, amount).
		Updates(map[string]interface{}{"balance_usdt_new": gorm.Expr("balance_usdt_new - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "withdraw"
	userBalanceRecode.CoinType = "usdt_2"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	return nil
}

// WithdrawUsdt .
func (ub *UserBalanceRepo) WithdrawUsdt(ctx context.Context, userId int64, amount int64, tmpRecommendUserIdsInt []int64) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_usdt>=?", userId, amount).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	if 0 < len(tmpRecommendUserIdsInt) {
		if err = ub.data.DB(ctx).Table("user_info").
			Where("user_id in (?)", tmpRecommendUserIdsInt).
			Updates(map[string]interface{}{"team_csd_balance": gorm.Expr("team_csd_balance - ?", amount)}).Error; nil != err {
			return errors.NotFound("user balance err", "user balance not found")
		}
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "withdraw"
	userBalanceRecode.CoinType = "usdt"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	return nil
}

// TranUsdt .
func (ub *UserBalanceRepo) TranUsdt(ctx context.Context, userId int64, toUserId int64, amount int64, tmpRecommendUserIdsInt []int64, tmpRecommendUserIdsInt2 []int64) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_usdt>=?", userId, amount).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", toUserId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", amount)}).Error; nil != err {
		return errors.NotFound("user balance err", "user balance not found")
	}

	if len(tmpRecommendUserIdsInt2) > 0 {
		if err = ub.data.DB(ctx).Table("user_info").
			Where("user_id in (?)", tmpRecommendUserIdsInt2).
			Updates(map[string]interface{}{"team_csd_balance": gorm.Expr("team_csd_balance + ?", amount)}).Error; nil != err {
			return errors.NotFound("user balance err", "user balance not found")
		}
	}
	if 0 < len(tmpRecommendUserIdsInt) {
		if err = ub.data.DB(ctx).Table("user_info").
			Where("user_id in (?)", tmpRecommendUserIdsInt).
			Updates(map[string]interface{}{"team_csd_balance": gorm.Expr("team_csd_balance - ?", amount)}).Error; nil != err {
			return errors.NotFound("user balance err", "user balance not found")
		}
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "tran"
	userBalanceRecode.CoinType = "usdt"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	var userBalanceRecode1 UserBalanceRecord
	userBalanceRecode1.UserId = toUserId
	userBalanceRecode1.Type = "tran_to"
	userBalanceRecode1.CoinType = "usdt"
	userBalanceRecode1.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode1).Error
	if err != nil {
		return err
	}

	return nil
}

func (ub *UserBalanceRepo) TradeUsdt(ctx context.Context, userId int64, amount int64) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_usdt>=?", userId, amount).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "withdraw"
	userBalanceRecode.CoinType = "usdt"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	return nil
}

// SetBalanceReward .
func (ub *UserBalanceRepo) SetBalanceReward(ctx context.Context, userId int64, amount int64) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_usdt>=?", userId, amount).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	now := time.Now().UTC()
	var balanceReward BalanceReward
	balanceReward.Amount = amount
	balanceReward.UserId = userId
	balanceReward.SetDate = now
	balanceReward.Status = 1
	balanceReward.LastRewardDate = now
	balanceReward.H = int64(now.Hour())
	balanceReward.M = int64(now.Minute())
	err = ub.data.DB(ctx).Table("balance_reward").Create(&balanceReward).Error
	if err != nil {
		return err
	}
	return nil
}

// UpdateBalanceReward .
func (ub *UserBalanceRepo) UpdateBalanceReward(ctx context.Context, userId int64, id int64, amount int64, status int64) error {
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	if res := ub.data.DB(ctx).Table("balance_reward").
		Where("id=? and status=?", id, 1).
		Updates(map[string]interface{}{"amount": gorm.Expr("amount - ?", amount), "status": status}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	return nil
}

// WithdrawC .
func (ub *UserBalanceRepo) WithdrawC(ctx context.Context, userId int64, amount int64) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_c>=?", userId, amount).
		Updates(map[string]interface{}{"balance_c": gorm.Expr("balance_c - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceDhb
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "withdraw"
	userBalanceRecode.CoinType = "ispay"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	return nil
}

// WithdrawDhb .
func (ub *UserBalanceRepo) WithdrawDhb(ctx context.Context, userId int64, amount int64) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_dhb>=?", userId, amount).
		Updates(map[string]interface{}{"balance_dhb": gorm.Expr("balance_dhb - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceDhb
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "withdraw"
	userBalanceRecode.CoinType = "dhb"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	return nil
}

// TranDhb .
func (ub *UserBalanceRepo) TranDhb(ctx context.Context, userId int64, toUserId int64, amount int64) error {
	var err error
	if res := ub.data.DB(ctx).Table("user_balance").
		Where("user_id=? and balance_dhb>=?", userId, amount).
		Updates(map[string]interface{}{"balance_dhb": gorm.Expr("balance_dhb - ?", amount)}); 0 == res.RowsAffected || nil != res.Error {
		return errors.NotFound("user balance err", "user balance error")
	}

	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", toUserId).
		Updates(map[string]interface{}{"balance_dhb": gorm.Expr("balance_dhb + ?", amount)}).Error; nil != err {
		return errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceDhb
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "tran"
	userBalanceRecode.CoinType = "dhb"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return err
	}

	var userBalanceRecode1 UserBalanceRecord
	userBalanceRecode1.UserId = toUserId
	userBalanceRecode1.Type = "tran_to"
	userBalanceRecode1.CoinType = "dhb"
	userBalanceRecode1.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode1).Error
	if err != nil {
		return err
	}

	return nil
}

// GreateWithdraw .
func (ub *UserBalanceRepo) GreateWithdraw(ctx context.Context, userId int64, relAmount float64, amount float64, coinType string, address string) (*biz.Withdraw, error) {
	var withdraw Withdraw
	withdraw.UserId = userId
	withdraw.AmountNew = amount
	withdraw.RelAmountNew = relAmount
	withdraw.Type = coinType
	withdraw.Status = "rewarded"
	withdraw.Address = address
	res := ub.data.DB(ctx).Table("withdraw").Create(&withdraw)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_WITHDRAW_ERROR", "提现记录创建失败")
	}

	var (
		reward Reward
		err    error
	)

	reward.UserId = userId
	reward.AmountNew = amount
	reward.AmountNewTwo = relAmount
	reward.Type = coinType // 本次分红的行为类型
	reward.TypeRecordId = withdraw.ID
	reward.Reason = "withdraw" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return nil, errors.New(500, "CREATE_WITHDRAW_ERROR", "提现记录创建失败")
	}

	return &biz.Withdraw{
		ID:              withdraw.ID,
		UserId:          withdraw.UserId,
		AmountNew:       withdraw.AmountNew,
		RelAmountNew:    withdraw.RelAmountNew,
		BalanceRecordId: withdraw.BalanceRecordId,
		Status:          withdraw.Status,
		Type:            withdraw.Type,
		CreatedAt:       withdraw.CreatedAt,
	}, nil
}

// UpdateWithdraw .
func (ub *UserBalanceRepo) UpdateWithdraw(ctx context.Context, id int64, status string) (*biz.Withdraw, error) {
	var withdraw Withdraw
	withdraw.Status = status
	res := ub.data.DB(ctx).Table("withdraw").Where("id=?", id).Updates(&withdraw)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_WITHDRAW_ERROR", "提现记录修改失败")
	}

	return &biz.Withdraw{
		ID:              withdraw.ID,
		UserId:          withdraw.UserId,
		Amount:          withdraw.Amount,
		RelAmount:       withdraw.RelAmount,
		BalanceRecordId: withdraw.BalanceRecordId,
		Status:          withdraw.Status,
		Type:            withdraw.Type,
		CreatedAt:       withdraw.CreatedAt,
	}, nil
}

// GetWithdrawByUserId2 .
func (ub *UserBalanceRepo) GetWithdrawByUserId2(ctx context.Context, userId int64) ([]*biz.Withdraw, error) {
	var withdraws []*Withdraw
	res := make([]*biz.Withdraw, 0)
	if err := ub.data.db.Where("user_id=?", userId).Table("withdraw").Find(&withdraws).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("WITHDRAW_NOT_FOUND", "withdraw not found")
		}

		return nil, errors.New(500, "WITHDRAW ERROR", err.Error())
	}

	for _, withdraw := range withdraws {
		res = append(res, &biz.Withdraw{
			ID:              withdraw.ID,
			UserId:          withdraw.UserId,
			Amount:          withdraw.Amount,
			RelAmount:       withdraw.RelAmount,
			BalanceRecordId: withdraw.BalanceRecordId,
			Status:          withdraw.Status,
			Type:            withdraw.Type,
			CreatedAt:       withdraw.CreatedAt,
			AmountNew:       withdraw.AmountNew,
			RelAmountNew:    withdraw.RelAmountNew,
			Address:         withdraw.Address,
		})
	}
	return res, nil
}

// GetWithdrawByUserId .
func (ub *UserBalanceRepo) GetWithdrawByUserId(ctx context.Context, userId int64, typeCoin string) ([]*biz.Withdraw, error) {
	var withdraws []*Withdraw
	res := make([]*biz.Withdraw, 0)
	if err := ub.data.db.Where("user_id=?", userId).Where("type=?", typeCoin).Table("withdraw").Find(&withdraws).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("WITHDRAW_NOT_FOUND", "withdraw not found")
		}

		return nil, errors.New(500, "WITHDRAW ERROR", err.Error())
	}

	for _, withdraw := range withdraws {
		res = append(res, &biz.Withdraw{
			ID:              withdraw.ID,
			UserId:          withdraw.UserId,
			Amount:          withdraw.Amount,
			RelAmount:       withdraw.RelAmount,
			BalanceRecordId: withdraw.BalanceRecordId,
			Status:          withdraw.Status,
			Type:            withdraw.Type,
			CreatedAt:       withdraw.CreatedAt,
		})
	}
	return res, nil
}

// GetUserBalanceRecordByUserId .
func (ub *UserBalanceRepo) GetUserBalanceRecordByUserId(ctx context.Context, userId int64, typeCoin string, tran string) ([]*biz.UserBalanceRecord, error) {
	var userBalanceRecord []*UserBalanceRecord
	res := make([]*biz.UserBalanceRecord, 0)
	if err := ub.data.db.Where("user_id=?", userId).
		Where("type=? and coin_type=?", tran, typeCoin).Table("user_balance_record").Find(&userBalanceRecord).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("WITHDRAW_NOT_FOUND", "withdraw not found")
		}

		return nil, errors.New(500, "WITHDRAW ERROR", err.Error())
	}

	for _, v := range userBalanceRecord {
		res = append(res, &biz.UserBalanceRecord{
			ID:        v.ID,
			UserId:    v.UserId,
			Amount:    v.Amount,
			Type:      v.Type,
			CreatedAt: v.CreatedAt,
			Balance:   v.Balance,
		})
	}
	return res, nil
}

// GetUserBalanceRecordsByUserId .
func (ub *UserBalanceRepo) GetUserBalanceRecordsByUserId(ctx context.Context, userId int64) ([]*biz.UserBalanceRecord, error) {
	var userBalanceRecord []*UserBalanceRecord
	res := make([]*biz.UserBalanceRecord, 0)
	if err := ub.data.db.Where("user_id=?", userId).
		Where("(type=? or type=?) and coin_type=?", "deposit_2", "deposit", "usdt").Table("user_balance_record").Find(&userBalanceRecord).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("WITHDRAW_NOT_FOUND", "withdraw not found")
		}

		return nil, errors.New(500, "WITHDRAW ERROR", err.Error())
	}

	for _, v := range userBalanceRecord {
		res = append(res, &biz.UserBalanceRecord{
			ID:        v.ID,
			UserId:    v.UserId,
			Amount:    v.Amount,
			CoinType:  v.CoinType,
			CreatedAt: v.CreatedAt,
		})
	}
	return res, nil
}

// GetTradeByUserId .
func (ub *UserBalanceRepo) GetTradeByUserId(ctx context.Context, userId int64) ([]*biz.Trade, error) {
	var trades []*Trade
	res := make([]*biz.Trade, 0)
	if err := ub.data.db.Where("user_id=?", userId).Table("trade").Find(&trades).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("WITHDRAW_NOT_FOUND", "withdraw not found")
		}

		return nil, errors.New(500, "WITHDRAW ERROR", err.Error())
	}

	for _, trade := range trades {
		res = append(res, &biz.Trade{
			ID:           trade.ID,
			UserId:       trade.UserId,
			AmountCsd:    trade.AmountCsd,
			RelAmountCsd: trade.RelAmountCsd,
			AmountHbs:    trade.AmountHbs,
			RelAmountHbs: trade.RelAmountHbs,
			Status:       trade.Status,
			CreatedAt:    trade.CreatedAt,
		})
	}
	return res, nil
}

// GetBalanceRewardByUserId .
func (ub *UserBalanceRepo) GetBalanceRewardByUserId(ctx context.Context, userId int64) ([]*biz.BalanceReward, error) {
	var balanceRewards []*BalanceReward
	res := make([]*biz.BalanceReward, 0)
	if err := ub.data.db.Where("user_id=?", userId).Where("status=?", 1).Order("id asc").Table("balance_reward").Find(&balanceRewards).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("WITHDRAW_NOT_FOUND", "withdraw not found")
		}

		return nil, errors.New(500, "WITHDRAW ERROR", err.Error())
	}

	for _, balanceReward := range balanceRewards {
		res = append(res, &biz.BalanceReward{
			ID:      balanceReward.ID,
			UserId:  balanceReward.UserId,
			Status:  balanceReward.Status,
			Amount:  balanceReward.Amount,
			SetDate: balanceReward.SetDate,
		})
	}
	return res, nil
}

// GetWithdrawNotDeal .
func (ub *UserBalanceRepo) GetWithdrawNotDeal(ctx context.Context) ([]*biz.Withdraw, error) {
	var withdraws []*Withdraw
	res := make([]*biz.Withdraw, 0)
	if err := ub.data.db.Where("status=?", "").Table("withdraw").Find(&withdraws).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("WITHDRAW_NOT_FOUND", "withdraw not found")
		}

		return nil, errors.New(500, "WITHDRAW ERROR", err.Error())
	}

	for _, withdraw := range withdraws {
		res = append(res, &biz.Withdraw{
			ID:              withdraw.ID,
			UserId:          withdraw.UserId,
			Amount:          withdraw.Amount,
			RelAmount:       withdraw.RelAmount,
			BalanceRecordId: withdraw.BalanceRecordId,
			Status:          withdraw.Status,
			Type:            withdraw.Type,
			CreatedAt:       withdraw.CreatedAt,
		})
	}
	return res, nil
}

func (ub *UserBalanceRepo) GetWithdrawById(ctx context.Context, id int64) (*biz.Withdraw, error) {
	var withdraw *Withdraw
	if err := ub.data.db.Where("id=?", id).Table("withdraw").First(&withdraw).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("WITHDRAW_NOT_FOUND", "withdraw not found")
		}

		return nil, errors.New(500, "WITHDRAW ERROR", err.Error())
	}
	return &biz.Withdraw{
		ID:              withdraw.ID,
		UserId:          withdraw.UserId,
		Amount:          withdraw.Amount,
		RelAmount:       withdraw.RelAmount,
		BalanceRecordId: withdraw.BalanceRecordId,
		Status:          withdraw.Status,
		Type:            withdraw.Type,
		CreatedAt:       withdraw.CreatedAt,
	}, nil
}

// GetWithdraws .
func (ub *UserBalanceRepo) GetWithdraws(ctx context.Context, b *biz.Pagination, userId int64) ([]*biz.Withdraw, error, int64) {
	var (
		withdraws []*Withdraw
		count     int64
	)
	res := make([]*biz.Withdraw, 0)

	instance := ub.data.db.Table("withdraw")

	if 0 < userId {
		instance = instance.Where("user_id=?", userId)
	}

	instance = instance.Count(&count)
	if err := instance.Scopes(Paginate(b.PageNum, b.PageSize)).Order("id desc").Find(&withdraws).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("WITHDRAW_NOT_FOUND", "withdraw not found"), 0
		}

		return nil, errors.New(500, "WITHDRAW ERROR", err.Error()), 0
	}

	for _, withdraw := range withdraws {
		res = append(res, &biz.Withdraw{
			ID:              withdraw.ID,
			UserId:          withdraw.UserId,
			Amount:          withdraw.Amount,
			RelAmount:       withdraw.RelAmount,
			BalanceRecordId: withdraw.BalanceRecordId,
			Status:          withdraw.Status,
			Type:            withdraw.Type,
			CreatedAt:       withdraw.CreatedAt,
		})
	}
	return res, nil, count
}

// GetWithdrawPassOrRewarded .
func (ub *UserBalanceRepo) GetWithdrawPassOrRewarded(ctx context.Context) ([]*biz.Withdraw, error) {
	var withdraws []*Withdraw
	res := make([]*biz.Withdraw, 0)
	if err := ub.data.db.Table("withdraw").Where("status=? or status=?", "pass", "rewarded").Find(&withdraws).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("WITHDRAW_NOT_FOUND", "withdraw not found")
		}

		return nil, errors.New(500, "WITHDRAW ERROR", err.Error())
	}

	for _, withdraw := range withdraws {
		res = append(res, &biz.Withdraw{
			ID:              withdraw.ID,
			UserId:          withdraw.UserId,
			Amount:          withdraw.Amount,
			RelAmount:       withdraw.RelAmount,
			BalanceRecordId: withdraw.BalanceRecordId,
			Status:          withdraw.Status,
			Type:            withdraw.Type,
			CreatedAt:       withdraw.CreatedAt,
		})
	}
	return res, nil
}

// RecommendReward .
func (ub *UserBalanceRepo) RecommendReward(ctx context.Context, userId int64, amount int64, locationId int64) (int64, error) {
	var err error
	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", amount)}).Error; nil != err {
		return 0, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "reward"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	var reward Reward
	reward.UserId = userBalance.UserId
	reward.Amount = amount
	reward.BalanceRecordId = userBalanceRecode.ID
	reward.Type = "location" // 本次分红的行为类型
	reward.TypeRecordId = locationId
	reward.Reason = "recommend_vip" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// SystemReward .
func (ub *UserBalanceRepo) SystemReward(ctx context.Context, amount int64, locationId int64) error {
	var (
		reward Reward
		err    error
	)
	reward.UserId = 999999999
	reward.Amount = amount
	reward.BalanceRecordId = 999999999
	reward.Type = "location" // 本次分红的行为类型
	reward.TypeRecordId = locationId
	reward.Reason = "system_reward" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return err
	}

	return nil
}

// SystemWithdrawReward .
func (ub *UserBalanceRepo) SystemWithdrawReward(ctx context.Context, amount int64, locationId int64) error {
	var (
		reward Reward
		err    error
	)
	reward.UserId = 999999999
	reward.Amount = amount
	reward.BalanceRecordId = 999999999
	reward.Type = "withdraw" // 本次分红的行为类型
	reward.TypeRecordId = locationId
	reward.Reason = "system_reward" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return err
	}

	return nil
}

// GetSystemYesterdayDailyReward .
func (ub *UserBalanceRepo) GetSystemYesterdayDailyReward(ctx context.Context) (*biz.Reward, error) {
	var reward Reward

	now := time.Now().UTC()
	var startDate time.Time
	var endDate time.Time
	if 14 <= now.Hour() {
		startDate = now
		endDate = now.AddDate(0, 0, 1)
	} else {
		startDate = now.AddDate(0, 0, -1)
		endDate = now
	}
	todayStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 14, 0, 0, 0, time.UTC)
	todayEnd := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 14, 0, 0, 0, time.UTC)

	if err := ub.data.db.
		Where("user_id=?", 999999999).
		Where("created_at>=?", todayStart).
		Where("created_at<?", todayEnd).
		Where("type=?", "system_fee_daily").
		Where("reason=?", "system_fee_daily").
		Table("reward").First(&reward).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("REWARD_NOT_FOUND", "reward not found")
		}

		return nil, errors.New(500, "REWARD ERROR", err.Error())
	}
	return &biz.Reward{
		ID:               reward.ID,
		UserId:           reward.UserId,
		Amount:           reward.Amount,
		BalanceRecordId:  reward.BalanceRecordId,
		Type:             reward.Type,
		TypeRecordId:     reward.TypeRecordId,
		Reason:           reward.Reason,
		ReasonLocationId: reward.ReasonLocationId,
		LocationType:     reward.LocationType,
	}, nil
}

// SystemFee .
func (ub *UserBalanceRepo) SystemFee(ctx context.Context, amount int64, locationId int64) error {
	var (
		reward Reward
		err    error
	)
	reward.UserId = 999999999
	reward.Amount = amount
	reward.BalanceRecordId = 999999999
	reward.Type = "withdraw" // 本次分红的行为类型
	reward.TypeRecordId = locationId
	reward.Reason = "system_fee" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return err
	}

	return nil
}

// UserFee .
func (ub *UserBalanceRepo) UserFee(ctx context.Context, userId int64, amount int64) (int64, error) {
	var err error
	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", amount)}).Error; nil != err {
		return 0, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "reward"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	var reward Reward
	reward.UserId = userBalance.UserId
	reward.Amount = amount
	reward.BalanceRecordId = userBalanceRecode.ID
	reward.Type = "system_reward" // 本次分红的行为类型
	reward.Reason = "fee"         // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// RecommendWithdrawReward .
func (ub *UserBalanceRepo) RecommendWithdrawReward(ctx context.Context, userId int64, amount int64, locationId int64) (int64, error) {
	var err error
	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", amount)}).Error; nil != err {
		return 0, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "reward"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	var reward Reward
	reward.UserId = userBalance.UserId
	reward.Amount = amount
	reward.BalanceRecordId = userBalanceRecode.ID
	reward.Type = "withdraw" // 本次分红的行为类型
	reward.TypeRecordId = locationId
	reward.Reason = "recommend_vip" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// NormalRecommendReward .
func (ub *UserBalanceRepo) NormalRecommendReward(ctx context.Context, userId int64, amount int64, locationId int64) (int64, error) {
	var err error
	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", amount)}).Error; nil != err {
		return 0, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "reward"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	var reward Reward
	reward.UserId = userBalance.UserId
	reward.Amount = amount
	reward.BalanceRecordId = userBalanceRecode.ID
	reward.Type = "location" // 本次分红的行为类型
	reward.TypeRecordId = locationId
	reward.Reason = "recommend" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// NormalWithdrawRecommendReward .
func (ub *UserBalanceRepo) NormalWithdrawRecommendReward(ctx context.Context, userId int64, amount int64, locationId int64) (int64, error) {
	var err error
	if err = ub.data.DB(ctx).Table("user_balance").
		Where("user_id=?", userId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", amount)}).Error; nil != err {
		return 0, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalance UserBalance
	err = ub.data.DB(ctx).Where(&UserBalance{UserId: userId}).Table("user_balance").First(&userBalance).Error
	if err != nil {
		return 0, err
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = userBalance.BalanceUsdt
	userBalanceRecode.UserId = userBalance.UserId
	userBalanceRecode.Type = "reward"
	userBalanceRecode.Amount = amount
	err = ub.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode).Error
	if err != nil {
		return 0, err
	}

	var reward Reward
	reward.UserId = userBalance.UserId
	reward.Amount = amount
	reward.BalanceRecordId = userBalanceRecode.ID
	reward.Type = "withdraw" // 本次分红的行为类型
	reward.TypeRecordId = locationId
	reward.Reason = "recommend" // 给我分红的理由
	err = ub.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return 0, err
	}

	return userBalanceRecode.ID, nil
}

// GetUserCurrentMonthRecommendByUserId .
func (uc *UserCurrentMonthRecommendRepo) GetUserCurrentMonthRecommendByUserId(ctx context.Context, userId int64) ([]*biz.UserCurrentMonthRecommend, error) {
	var userCurrentMonthRecommends []*UserCurrentMonthRecommend
	res := make([]*biz.UserCurrentMonthRecommend, 0)
	if err := uc.data.db.Where(&UserCurrentMonthRecommend{UserId: userId}).Table("user_current_month_recommend").Find(&userCurrentMonthRecommends).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USER_CURRENT_MONTH_RECOMMEND_NOT_FOUND", "user current month recommend not found")
		}

		return nil, errors.New(500, "USER CURRENT MONTH RECOMMEND ERROR", err.Error())
	}

	for _, userCurrentMonthRecommend := range userCurrentMonthRecommends {
		res = append(res, &biz.UserCurrentMonthRecommend{
			ID:              userCurrentMonthRecommend.ID,
			UserId:          userCurrentMonthRecommend.UserId,
			RecommendUserId: userCurrentMonthRecommend.RecommendUserId,
			Date:            userCurrentMonthRecommend.Date,
		})
	}
	return res, nil
}

// GetUserCurrentMonthRecommendGroupByUserId .
func (uc *UserCurrentMonthRecommendRepo) GetUserCurrentMonthRecommendGroupByUserId(ctx context.Context, b *biz.Pagination, userId int64) ([]*biz.UserCurrentMonthRecommend, error, int64) {
	var (
		count                      int64
		userCurrentMonthRecommends []*UserCurrentMonthRecommend
	)
	res := make([]*biz.UserCurrentMonthRecommend, 0)

	instance := uc.data.db.Table("user_current_month_recommend")
	if 0 < userId {
		instance = instance.Where("user_id=?", userId)
	}

	instance = instance.Count(&count)
	if err := instance.Scopes(Paginate(b.PageNum, b.PageSize)).Find(&userCurrentMonthRecommends).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USER_CURRENT_MONTH_RECOMMEND_NOT_FOUND", "user current month recommend not found"), 0
		}

		return nil, errors.New(500, "USER CURRENT MONTH RECOMMEND ERROR", err.Error()), 0
	}

	for _, userCurrentMonthRecommend := range userCurrentMonthRecommends {
		res = append(res, &biz.UserCurrentMonthRecommend{
			ID:              userCurrentMonthRecommend.ID,
			UserId:          userCurrentMonthRecommend.UserId,
			RecommendUserId: userCurrentMonthRecommend.RecommendUserId,
			Date:            userCurrentMonthRecommend.Date,
		})
	}
	return res, nil, count
}

// GetUserRewardDeposit .
func (ub *UserBalanceRepo) GetUserRewardDeposit(ctx context.Context, userId int64) ([]*biz.Reward, error) {
	var rewards []*Reward
	res := make([]*biz.Reward, 0)
	if err := ub.data.db.Table("reward").Where("reason=?", "deposit").
		Limit(100).Order("id desc").Find(&rewards).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("REWARD_NOT_FOUND", "reward not found")
		}
		return nil, errors.New(500, "REWARD ERROR", err.Error())
	}

	for _, reward := range rewards {
		res = append(res, &biz.Reward{
			ID:               reward.ID,
			UserId:           reward.UserId,
			Amount:           reward.Amount,
			BalanceRecordId:  reward.BalanceRecordId,
			Type:             reward.Type,
			TypeRecordId:     reward.TypeRecordId,
			Reason:           reward.Reason,
			ReasonLocationId: reward.ReasonLocationId,
			LocationType:     reward.LocationType,
			CreatedAt:        reward.CreatedAt,
			AmountB:          reward.AmountB,
			AmountNew:        reward.AmountNew,
			AmountNewTwo:     reward.AmountNewTwo,
			Address:          reward.Address,
		})
	}
	return res, nil
}

// GetUserRewardByUserId .
func (ub *UserBalanceRepo) GetUserRewardByUserId(ctx context.Context, userId int64) ([]*biz.Reward, error) {
	var rewards []*Reward
	res := make([]*biz.Reward, 0)
	if err := ub.data.db.Where("user_id", userId).Table("reward").Limit(10000).Order("id desc").Find(&rewards).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("REWARD_NOT_FOUND", "reward not found")
		}

		return nil, errors.New(500, "REWARD ERROR", err.Error())
	}

	for _, reward := range rewards {
		res = append(res, &biz.Reward{
			ID:               reward.ID,
			UserId:           reward.UserId,
			Amount:           reward.Amount,
			BalanceRecordId:  reward.BalanceRecordId,
			Type:             reward.Type,
			TypeRecordId:     reward.TypeRecordId,
			Reason:           reward.Reason,
			ReasonLocationId: reward.ReasonLocationId,
			LocationType:     reward.LocationType,
			CreatedAt:        reward.CreatedAt,
			AmountB:          reward.AmountB,
			AmountNew:        reward.AmountNew,
			AmountNewTwo:     reward.AmountNewTwo,
			Address:          reward.Address,
		})
	}
	return res, nil
}

// GetUserRewardByUserIds .
func (ub *UserBalanceRepo) GetUserRewardByUserIds(ctx context.Context, userIds ...int64) (map[int64]*biz.UserSortRecommendReward, error) {
	var total []*UserSortRecommendReward
	res := make(map[int64]*biz.UserSortRecommendReward, 0)

	if err := ub.data.db.Table("reward").
		Where("user_id IN(?)", userIds).
		Group("user_id").
		Select("sum(amount) as total, user_id").
		Scopes(Paginate(1, 4)).
		Take(&total).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("REWARD_NOT_FOUND", "reward not found")
		}
		return res, errors.New(500, "REWARD ERROR", err.Error())
	}

	for _, v := range total {
		res[v.UserId] = &biz.UserSortRecommendReward{
			Total:  v.Total,
			UserId: v.UserId,
		}
	}

	return res, nil
}

// GetUserRewardTodayTotalByUserId .
func (ub *UserBalanceRepo) GetUserRewardTodayTotalByUserId(ctx context.Context, userId int64) (*biz.UserSortRecommendReward, error) {
	var total *UserSortRecommendReward

	now := time.Now().UTC()
	var startDate time.Time
	var endDate time.Time
	if 14 <= now.Hour() {
		startDate = now
		endDate = now.AddDate(0, 0, 1)
	} else {
		startDate = now.AddDate(0, 0, -1)
		endDate = now
	}

	todayStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 14, 0, 0, 0, time.UTC)
	todayEnd := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 14, 0, 0, 0, time.UTC)

	if err := ub.data.db.Table("reward").
		Where("user_id=?", userId).
		Where("created_at>=?", todayStart).Where("created_at<?", todayEnd).
		Select("sum(amount) as total, user_id").
		Take(&total).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("REWARD_NOT_FOUND", "reward not found")
		}
		return nil, errors.New(500, "REWARD ERROR", err.Error())
	}

	return &biz.UserSortRecommendReward{
		UserId: total.UserId,
		Total:  total.Total,
	}, nil
}

// GetUserRewards .
func (ub *UserBalanceRepo) GetUserRewards(ctx context.Context, b *biz.Pagination, userId int64) ([]*biz.Reward, error, int64) {
	var (
		rewards []*Reward
		count   int64
	)
	res := make([]*biz.Reward, 0)

	instance := ub.data.db.Table("reward")

	if 0 < userId {
		instance = instance.Where("user_id=?", userId)
	}

	instance = instance.Count(&count)
	if err := instance.Scopes(Paginate(b.PageNum, b.PageSize)).Order("id desc").Find(&rewards).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("REWARD_NOT_FOUND", "reward not found"), 0
		}

		return nil, errors.New(500, "REWARD ERROR", err.Error()), 0
	}

	for _, reward := range rewards {
		res = append(res, &biz.Reward{
			ID:               reward.ID,
			UserId:           reward.UserId,
			Amount:           reward.Amount,
			BalanceRecordId:  reward.BalanceRecordId,
			Type:             reward.Type,
			TypeRecordId:     reward.TypeRecordId,
			Reason:           reward.Reason,
			ReasonLocationId: reward.ReasonLocationId,
			LocationType:     reward.LocationType,
			CreatedAt:        reward.CreatedAt,
		})
	}
	return res, nil, count
}

// GetUserRewardsLastMonthFee .
func (ub *UserBalanceRepo) GetUserRewardsLastMonthFee(ctx context.Context) ([]*biz.Reward, error) {
	var (
		rewards []*Reward
	)
	res := make([]*biz.Reward, 0)

	instance := ub.data.db.Table("reward")

	now := time.Now().UTC().Add(8 * time.Hour)
	lastMonthFirstDay := now.AddDate(0, -1, -now.Day()+1)
	lastMonthStart := time.Date(lastMonthFirstDay.Year(), lastMonthFirstDay.Month(), lastMonthFirstDay.Day(), 0, 0, 0, 0, time.UTC)
	lastMonthEndDay := lastMonthFirstDay.AddDate(0, 1, -1)
	lastMonthEnd := time.Date(lastMonthEndDay.Year(), lastMonthEndDay.Month(), lastMonthEndDay.Day(), 23, 59, 59, 0, time.UTC)

	if err := instance.Where("created_at>=?", lastMonthStart).
		Where("created_at<=?", lastMonthEnd).
		Where("reason=?", "system_fee").
		Find(&rewards).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("REWARD_NOT_FOUND", "reward not found")
		}

		return nil, errors.New(500, "REWARD ERROR", err.Error())
	}

	for _, reward := range rewards {
		res = append(res, &biz.Reward{
			ID:               reward.ID,
			UserId:           reward.UserId,
			Amount:           reward.Amount,
			BalanceRecordId:  reward.BalanceRecordId,
			Type:             reward.Type,
			TypeRecordId:     reward.TypeRecordId,
			Reason:           reward.Reason,
			ReasonLocationId: reward.ReasonLocationId,
			LocationType:     reward.LocationType,
			CreatedAt:        reward.CreatedAt,
		})
	}
	return res, nil
}

// CreateUserCurrentMonthRecommend .
func (uc *UserCurrentMonthRecommendRepo) CreateUserCurrentMonthRecommend(ctx context.Context, u *biz.UserCurrentMonthRecommend) (*biz.UserCurrentMonthRecommend, error) {
	var userCurrentMonthRecommend UserCurrentMonthRecommend
	userCurrentMonthRecommend.UserId = u.UserId
	userCurrentMonthRecommend.RecommendUserId = u.RecommendUserId
	userCurrentMonthRecommend.Date = u.Date
	res := uc.data.DB(ctx).Table("user_current_month_recommend").Create(&userCurrentMonthRecommend)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_USER_CURRENT_MONTH_RECOMMEND_ERROR", "用户当月推荐人创建失败")
	}

	return &biz.UserCurrentMonthRecommend{
		ID:              userCurrentMonthRecommend.ID,
		UserId:          userCurrentMonthRecommend.UserId,
		RecommendUserId: userCurrentMonthRecommend.RecommendUserId,
		Date:            userCurrentMonthRecommend.Date,
	}, nil
}

// GetUserBalanceByUserIds .
func (ub UserBalanceRepo) GetUserBalanceByUserIds(ctx context.Context, userIds ...int64) (map[int64]*biz.UserBalance, error) {
	var userBalances []*UserBalance
	res := make(map[int64]*biz.UserBalance)
	if err := ub.data.db.Where("user_id IN (?)", userIds).Table("user_balance").Find(&userBalances).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USER_BALANCE_NOT_FOUND", "user balance not found")
		}

		return nil, errors.New(500, "USER BALANCE ERROR", err.Error())
	}

	for _, userBalance := range userBalances {
		res[userBalance.UserId] = &biz.UserBalance{
			ID:          userBalance.ID,
			UserId:      userBalance.UserId,
			BalanceUsdt: userBalance.BalanceUsdt,
			BalanceDhb:  userBalance.BalanceDhb,
		}
	}

	return res, nil
}

type UserBalanceTotal struct {
	Total int64
}

type UserSortRecommendReward struct {
	UserId int64
	Total  int64
}

// GetUserBalanceUsdtTotal .
func (ub UserBalanceRepo) GetUserBalanceUsdtTotal(ctx context.Context) (int64, error) {
	var total UserBalanceTotal
	if err := ub.data.db.Table("user_balance").Select("sum(balance_usdt) as total").Take(&total).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return total.Total, errors.NotFound("USER_BALANCE_NOT_FOUND", "user balance not found")
		}

		return total.Total, errors.New(500, "USER BALANCE ERROR", err.Error())
	}

	return total.Total, nil
}

// GetUserBalanceRecordUsdtTotal .
func (ub UserBalanceRepo) GetUserBalanceRecordUsdtTotal(ctx context.Context) (int64, error) {
	var total UserBalanceTotal
	if err := ub.data.db.Table("user_balance_record").
		Where("type=?", "deposit").
		Where("coin_type=?", "usdt").
		Select("sum(amount) as total").Take(&total).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return total.Total, errors.NotFound("USER_BALANCE_RECORD_NOT_FOUND", "user balance not found")
		}

		return total.Total, errors.New(500, "USER BALANCE RECORD ERROR", err.Error())
	}

	return total.Total, nil
}

// GetUserBalanceRecordUserUsdtTotal .
func (ub UserBalanceRepo) GetUserBalanceRecordUserUsdtTotal(ctx context.Context, userId int64) (int64, error) {
	var total UserBalanceTotal
	if err := ub.data.db.Table("user_balance_record").
		Where("user_id=?", userId).
		Where("type=?", "deposit").
		Where("coin_type=?", "usdt").
		Select("sum(amount) as total").Take(&total).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return total.Total, errors.NotFound("USER_BALANCE_RECORD_NOT_FOUND", "user balance not found")
		}

		return total.Total, errors.New(500, "USER BALANCE RECORD ERROR", err.Error())
	}

	return total.Total, nil
}

// GetLocationsToday .
func (ub UserBalanceRepo) GetLocationsToday(ctx context.Context) ([]*biz.LocationNew, error) {
	var locations []*LocationNew

	now := time.Now().UTC()
	var startDate time.Time
	var endDate time.Time
	if 14 <= now.Hour() {
		startDate = now
		endDate = now.AddDate(0, 0, 1)
	} else {
		startDate = now.AddDate(0, 0, -1)
		endDate = now
	}
	todayStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 16, 0, 0, 0, time.UTC)
	todayEnd := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 15, 59, 59, 0, time.UTC)

	res := make([]*biz.LocationNew, 0)
	if err := ub.data.db.Table("location_new").
		Where("created_at>=?", todayStart).Where("created_at<=?", todayEnd).
		Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, nil
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.LocationNew{
			ID:            location.ID,
			UserId:        location.UserId,
			Status:        location.Status,
			Current:       location.Current,
			CurrentMax:    location.CurrentMax,
			CreatedAt:     location.CreatedAt,
			CurrentMaxNew: location.CurrentMaxNew,
			Usdt:          location.Usdt,
			Num:           location.Num,
			Total:         location.Total,
			TotalTwo:      location.TotalTwo,
			TotalThree:    location.TotalThree,
			Biw:           location.Biw,
			LastLevel:     location.LastLevel,
		})
	}

	return res, nil
}

// GetUserBalanceRecordUsdtTotalToday .
func (ub UserBalanceRepo) GetUserBalanceRecordUsdtTotalToday(ctx context.Context) (int64, error) {
	var total UserBalanceTotal

	now := time.Now().UTC()
	var startDate time.Time
	var endDate time.Time
	if 14 <= now.Hour() {
		startDate = now
		endDate = now.AddDate(0, 0, 1)
	} else {
		startDate = now.AddDate(0, 0, -1)
		endDate = now
	}
	todayStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 16, 0, 0, 0, time.UTC)
	todayEnd := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 15, 59, 59, 0, time.UTC)

	if err := ub.data.db.Table("user_balance_record").
		Where("type=?", "deposit").
		Where("coin_type=?", "usdt").
		Where("created_at>=?", todayStart).Where("created_at<=?", todayEnd).
		Select("sum(amount) as total").Take(&total).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return total.Total, errors.NotFound("USER_BALANCE_RECORD_NOT_FOUND", "user balance not found")
		}

		return total.Total, errors.New(500, "USER BALANCE RECORD ERROR", err.Error())
	}

	return total.Total, nil
}

// GetUserRewardRecommendSort .
func (ub *UserBalanceRepo) GetUserRewardRecommendSort(ctx context.Context) ([]*biz.UserSortRecommendReward, error) {
	var total []*UserSortRecommendReward
	res := make([]*biz.UserSortRecommendReward, 0)

	now := time.Now().UTC()
	var startDate time.Time
	var endDate time.Time
	if 14 <= now.Hour() {
		startDate = now
		endDate = now.AddDate(0, 0, 1)
	} else {
		startDate = now.AddDate(0, 0, -1)
		endDate = now
	}
	todayStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 14, 0, 0, 0, time.UTC)
	todayEnd := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 14, 0, 0, 0, time.UTC)

	if err := ub.data.db.Table("reward").
		Where("type=?", "location").
		Where("reason=?", "recommend").
		Where("created_at>=?", todayStart).
		Where("created_at<?", todayEnd).
		Group("user_id").
		Select("sum(amount) as total, user_id").
		Order("total desc").
		Scopes(Paginate(1, 4)).
		Take(&total).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("REWARD_NOT_FOUND", "reward not found")
		}
		return res, errors.New(500, "REWARD ERROR", err.Error())
	}

	for _, v := range total {
		res = append(res, &biz.UserSortRecommendReward{
			Total:  v.Total,
			UserId: v.UserId,
		})
	}

	return res, nil
}

// GetUserWithdrawUsdtTotalToday .
func (ub UserBalanceRepo) GetUserWithdrawUsdtTotalToday(ctx context.Context) (int64, error) {
	var total UserBalanceTotal
	now := time.Now().UTC()
	var startDate time.Time
	var endDate time.Time
	if 14 <= now.Hour() {
		startDate = now
		endDate = now.AddDate(0, 0, 1)
	} else {
		startDate = now.AddDate(0, 0, -1)
		endDate = now
	}
	todayStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 16, 0, 0, 0, time.UTC)
	todayEnd := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 15, 59, 59, 0, time.UTC)
	if err := ub.data.db.Table("user_balance_record").
		Where("type=?", "withdraw").
		Where("coin_type=?", "usdt").
		Where("created_at>=?", todayStart).Where("created_at<=?", todayEnd).
		Select("sum(amount) as total").Take(&total).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return total.Total, errors.NotFound("USER_BALANCE_RECORD_NOT_FOUND", "user balance not found")
		}

		return total.Total, errors.New(500, "USER BALANCE RECORD ERROR", err.Error())
	}

	return total.Total, nil
}

// GetUserWithdrawUsdtTotal .
func (ub UserBalanceRepo) GetUserWithdrawUsdtTotal(ctx context.Context) (int64, error) {
	var total UserBalanceTotal
	if err := ub.data.db.Table("user_balance_record").
		Where("type=?", "withdraw").
		Where("coin_type=?", "usdt").
		Select("sum(amount) as total").Take(&total).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return total.Total, errors.NotFound("USER_BALANCE_RECORD_NOT_FOUND", "user balance not found")
		}

		return total.Total, errors.New(500, "USER BALANCE RECORD ERROR", err.Error())
	}

	return total.Total, nil
}

// GetUserRewardUsdtTotal .
func (ub UserBalanceRepo) GetUserRewardUsdtTotal(ctx context.Context) (int64, error) {
	var total UserBalanceTotal
	if err := ub.data.db.Table("user_balance_record").
		Where("type=?", "reward").
		Select("sum(amount) as total").Take(&total).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return total.Total, errors.NotFound("USER_BALANCE_RECORD_NOT_FOUND", "user balance not found")
		}

		return total.Total, errors.New(500, "USER BALANCE RECORD ERROR", err.Error())
	}

	return total.Total, nil
}

// GetSystemRewardUsdtTotal .
func (ub UserBalanceRepo) GetSystemRewardUsdtTotal(ctx context.Context) (int64, error) {
	var total UserBalanceTotal
	if err := ub.data.db.Table("reward").
		Where("reason=? or reason=?", "system_reward", "system_fee").
		Select("sum(amount) as total").Take(&total).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return total.Total, errors.NotFound("USER_BALANCE_RECORD_NOT_FOUND", "user balance not found")
		}

		return total.Total, errors.New(500, "USER BALANCE RECORD ERROR", err.Error())
	}

	return total.Total, nil
}

// GetUserInfoByUserIds .
func (ui *UserInfoRepo) GetUserInfoByUserIds(ctx context.Context, userIds ...int64) (map[int64]*biz.UserInfo, error) {
	var userInfos []*UserInfo
	res := make(map[int64]*biz.UserInfo, 0)
	if err := ui.data.db.Where("user_id IN (?)", userIds).Table("user_info").Find(&userInfos).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USERINFO_NOT_FOUND", "userinfo not found")
		}

		return nil, errors.New(500, "USERINFO ERROR", err.Error())
	}

	for _, userInfo := range userInfos {
		res[userInfo.UserId] = &biz.UserInfo{
			ID:               userInfo.ID,
			UserId:           userInfo.UserId,
			Vip:              userInfo.Vip,
			HistoryRecommend: userInfo.HistoryRecommend,
			TeamCsdBalance:   userInfo.TeamCsdBalance,
		}
	}

	return res, nil
}

// GetUserCurrentMonthRecommendCountByUserIds .
func (uc *UserCurrentMonthRecommendRepo) GetUserCurrentMonthRecommendCountByUserIds(ctx context.Context, userIds ...int64) (map[int64]int64, error) {
	var userCurrentMonthRecommends []*UserCurrentMonthRecommend
	res := make(map[int64]int64, 0)
	if err := uc.data.db.Where("user_id IN (?)", userIds).Table("user_current_month_recommend").Find(&userCurrentMonthRecommends).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USER_CURRENT_MONTH_RECOMMEND_NOT_FOUND", "user current month recommend not found")
		}

		return nil, errors.New(500, "USER CURRENT MONTH RECOMMEND ERROR", err.Error())
	}

	for _, userCurrentMonthRecommend := range userCurrentMonthRecommends {
		if _, ok := res[userCurrentMonthRecommend.UserId]; !ok {
			res[userCurrentMonthRecommend.UserId] = 1
		} else {
			res[userCurrentMonthRecommend.UserId]++
		}
	}
	return res, nil
}

// GetUserLastMonthRecommend .
func (uc *UserCurrentMonthRecommendRepo) GetUserLastMonthRecommend(ctx context.Context) ([]int64, error) {
	var userCurrentMonthRecommends []*UserCurrentMonthRecommend
	res := make([]int64, 0)

	now := time.Now().UTC().Add(8 * time.Hour)
	lastMonthFirstDay := now.AddDate(0, -1, -now.Day()+1)
	lastMonthStart := time.Date(lastMonthFirstDay.Year(), lastMonthFirstDay.Month(), lastMonthFirstDay.Day(), 0, 0, 0, 0, time.UTC)
	lastMonthEndDay := lastMonthFirstDay.AddDate(0, 1, -1)
	lastMonthEnd := time.Date(lastMonthEndDay.Year(), lastMonthEndDay.Month(), lastMonthEndDay.Day(), 23, 59, 59, 0, time.UTC)

	if err := uc.data.db.Table("user_current_month_recommend").
		Group("user_id").
		Having("count(id) >= 5").
		Where("date>=?", lastMonthStart).
		Where("date<=?", lastMonthEnd).
		Find(&userCurrentMonthRecommends).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("USER_CURRENT_MONTH_RECOMMEND_NOT_FOUND", "user current month recommend not found")
		}

		return nil, errors.New(500, "USER CURRENT MONTH RECOMMEND ERROR", err.Error())
	}

	for _, userCurrentMonthRecommend := range userCurrentMonthRecommends {
		res = append(res, userCurrentMonthRecommend.UserId)
	}
	return res, nil
}
