package data

import (
	"context"
	"dhb/app/app/internal/biz"
	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
	"time"
)

type Location struct {
	ID           int64     `gorm:"primarykey;type:int"`
	UserId       int64     `gorm:"type:int;not null"`
	Row          int64     `gorm:"type:int;not null"`
	Col          int64     `gorm:"type:int;not null"`
	Status       string    `gorm:"type:varchar(45);not null"`
	CurrentLevel int64     `gorm:"type:int;not null"`
	Current      int64     `gorm:"type:bigint;not null"`
	Usdt         int64     `gorm:"type:bigint;not null"`
	CurrentMax   int64     `gorm:"type:bigint;not null"`
	StopDate     time.Time `gorm:"type:datetime;not null"`
	CreatedAt    time.Time `gorm:"type:datetime;not null"`
	UpdatedAt    time.Time `gorm:"type:datetime;not null"`
}

type LocationNew struct {
	ID                int64     `gorm:"primarykey;type:int"`
	UserId            int64     `gorm:"type:int;not null"`
	Num               int64     `gorm:"type:int;not null"`
	Status            string    `gorm:"type:varchar(45);not null"`
	Current           int64     `gorm:"type:bigint;not null"`
	CurrentMax        int64     `gorm:"type:bigint;not null"`
	Usdt              int64     `gorm:"type:bigint;not null"`
	CurrentMaxNew     int64     `gorm:"type:bigint;not null"`
	Count             int64     `gorm:"type:int;not null"`
	StopLocationAgain int64     `gorm:"type:int;not null"`
	OutRate           int64     `gorm:"type:int;not null"`
	Top               int64     `gorm:"type:int;not null"`
	TopNum            int64     `gorm:"type:int;not null"`
	Total             int64     `gorm:"type:bigint;not null"`
	TotalTwo          int64     `gorm:"type:bigint;not null"`
	TotalThree        int64     `gorm:"type:bigint;not null"`
	Biw               int64     `gorm:"type:bigint;not null"`
	StopCoin          int64     `gorm:"type:bigint;not null"`
	LastLevel         int64     `gorm:"type:bigint;not null"`
	StopDate          time.Time `gorm:"type:datetime;not null"`
	CreatedAt         time.Time `gorm:"type:datetime;not null"`
	UpdatedAt         time.Time `gorm:"type:datetime;not null"`
}

type GlobalLock struct {
	ID     int64 `gorm:"primarykey;type:int"`
	Status int64 `gorm:"type:int;not null"`
}

type LocationRepo struct {
	data *Data
	log  *log.Helper
}

func NewLocationRepo(data *Data, logger log.Logger) biz.LocationRepo {
	return &LocationRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

// CreateLocation .
func (lr *LocationRepo) CreateLocation(ctx context.Context, rel *biz.Location) (*biz.Location, error) {
	var location Location
	location.Col = rel.Col
	location.Row = rel.Row
	location.Status = rel.Status
	location.Current = rel.Current
	location.CurrentMax = rel.CurrentMax
	location.CurrentLevel = rel.CurrentLevel
	location.UserId = rel.UserId
	res := lr.data.DB(ctx).Table("location").Create(&location)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_LOCATION_ERROR", "占位信息创建失败")
	}

	return &biz.Location{
		ID:           location.ID,
		UserId:       location.UserId,
		Status:       location.Status,
		CurrentLevel: location.CurrentLevel,
		Current:      location.Current,
		CurrentMax:   location.CurrentMax,
		Row:          location.Row,
		Col:          location.Col,
	}, nil
}

// GetLocationLast .
func (lr *LocationRepo) GetLocationLast(ctx context.Context) (*biz.Location, error) {
	var location Location
	if err := lr.data.db.Table("location").Order("id desc").First(&location).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	return &biz.Location{
		ID:           location.ID,
		UserId:       location.UserId,
		Status:       location.Status,
		CurrentLevel: location.CurrentLevel,
		Current:      location.Current,
		CurrentMax:   location.CurrentMax,
		Row:          location.Row,
		Col:          location.Col,
	}, nil
}

// UpdateLocationNewCountTwo .
func (lr *LocationRepo) UpdateLocationNewCountTwo(ctx context.Context, id int64, count int64, total int64) error {

	if 1 == count {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"total": gorm.Expr("total + ?", total)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	} else if 2 == count {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"total_two": gorm.Expr("total_two + ?", total)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	} else if 3 == count {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"total_three": gorm.Expr("total_three + ?", total)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	}

	return nil
}

// UpdateLocationNewCount .
func (lr *LocationRepo) UpdateLocationNewCount(ctx context.Context, id int64, count int64, total int64) error {

	if 1 == count {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"count": count, "total": gorm.Expr("total + ?", total)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	} else if 2 == count {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"count": count, "total_two": gorm.Expr("total_two + ?", total)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	} else if 3 == count {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"count": count, "total_three": gorm.Expr("total_three + ?", total)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	}

	return nil
}

// UpdateLocationNewTotal .
func (lr *LocationRepo) UpdateLocationNewTotal(ctx context.Context, id int64, count int64, total int64) error {

	if 1 == count {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"total": gorm.Expr("total + ?", total)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	} else if 2 == count {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"count": count, "total_two": gorm.Expr("total_two + ?", total)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	} else if 3 == count {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"count": count, "total_three": gorm.Expr("total_three + ?", total)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	}
	return nil
}

// UpdateLocationNew .
func (lr *LocationRepo) UpdateLocationNew(ctx context.Context, id int64, userId int64, currentMax int64, amount int64, amountB int64, address string, coinType string) (*biz.LocationNew, error) {
	res := lr.data.DB(ctx).Table("location_new").
		Where("id=?", id).
		Updates(map[string]interface{}{
			"status":          "running",
			"num":             1,
			"current":         0,
			"current_max":     currentMax,
			"stop_date":       "0000-00-00 00:00:00",
			"usdt":            amount,
			"biw":             0,
			"current_max_new": 0,
		})
	if 0 == res.RowsAffected || res.Error != nil {
		return nil, errors.New(500, "UPDATE_LOCATION_ERROR", "占位信息创建失败")
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = 0
	userBalanceRecode.UserId = userId
	userBalanceRecode.Type = "deposit"
	userBalanceRecode.CoinType = coinType
	userBalanceRecode.Amount = amount
	res = lr.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_LOCATION_ERROR", "占位信息创建失败")
	}

	var (
		err    error
		reward Reward
	)

	reward.UserId = userId
	reward.Amount = amountB
	reward.Address = address
	reward.Type = coinType // 本次分红的行为类型
	reward.TypeRecordId = userBalanceRecode.ID
	reward.Reason = "buy" // 给我分红的理由
	err = lr.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return nil, errors.New(500, "CREATE_LOCATION_ERROR", "占位信息创建失败")
	}

	return nil, nil
}

// CreateLocationNew .
func (lr *LocationRepo) CreateLocationNew(ctx context.Context, rel *biz.LocationNew, amount int64, amountB int64, address string, coinType string) (*biz.LocationNew, error) {
	var location LocationNew
	location.Status = rel.Status
	location.Num = rel.Num
	location.Current = rel.Current
	location.CurrentMax = rel.CurrentMax
	location.UserId = rel.UserId
	location.OutRate = rel.OutRate
	location.StopDate = rel.StopDate
	location.Usdt = amount
	location.Top = rel.Top
	location.TopNum = rel.TopNum
	location.LastLevel = rel.LastLevel
	res := lr.data.DB(ctx).Table("location_new").Create(&location)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_LOCATION_ERROR", "占位信息创建失败")
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = 0
	userBalanceRecode.UserId = rel.UserId
	userBalanceRecode.Type = "deposit"
	userBalanceRecode.CoinType = coinType
	userBalanceRecode.Amount = amount
	res = lr.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_LOCATION_ERROR", "占位信息创建失败")
	}

	var (
		err    error
		reward Reward
	)

	reward.UserId = rel.UserId
	reward.Amount = amountB
	reward.Address = address
	reward.Type = coinType // 本次分红的行为类型
	reward.TypeRecordId = userBalanceRecode.ID
	reward.Reason = "buy" // 给我分红的理由
	err = lr.data.DB(ctx).Table("reward").Create(&reward).Error
	if err != nil {
		return nil, errors.New(500, "CREATE_LOCATION_ERROR", "占位信息创建失败")
	}

	return &biz.LocationNew{
		ID:         location.ID,
		UserId:     location.UserId,
		Status:     location.Status,
		Current:    location.Current,
		CurrentMax: location.CurrentMax,
		Num:        location.Num,
	}, nil
}

// GetMyLocationLast .
func (lr *LocationRepo) GetMyLocationLast(ctx context.Context, userId int64) (*biz.Location, error) {
	var location Location
	if err := lr.data.db.Table("location").Where("user_id", userId).Order("id desc").First(&location).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	return &biz.Location{
		ID:           location.ID,
		UserId:       location.UserId,
		Status:       location.Status,
		CurrentLevel: location.CurrentLevel,
		Current:      location.Current,
		CurrentMax:   location.CurrentMax,
		Row:          location.Row,
		Col:          location.Col,
		StopDate:     location.StopDate,
	}, nil
}

// GetMyStopLocationsLast .
func (lr *LocationRepo) GetMyStopLocationsLast(ctx context.Context, userId int64) ([]*biz.LocationNew, error) {

	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	if err := lr.data.db.Table("location_new").
		Where("user_id", userId).
		Where("status=?", "stop").
		Where("stop_location_again", 0).
		Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.LocationNew{
			ID:                location.ID,
			UserId:            location.UserId,
			Status:            location.Status,
			Current:           location.Current,
			CurrentMax:        location.CurrentMax,
			StopDate:          location.StopDate,
			StopLocationAgain: location.StopLocationAgain,
			StopCoin:          location.StopCoin,
		})
	}

	return res, nil
}

// GetMyStopLocationLast .
func (lr *LocationRepo) GetMyStopLocationLast(ctx context.Context, userId int64) (*biz.Location, error) {
	var location Location
	if err := lr.data.db.Table("location").
		Where("status=?", "stop").
		Where("user_id", userId).Order("id desc").First(&location).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	return &biz.Location{
		ID:           location.ID,
		UserId:       location.UserId,
		Status:       location.Status,
		CurrentLevel: location.CurrentLevel,
		Current:      location.Current,
		CurrentMax:   location.CurrentMax,
		Row:          location.Row,
		Col:          location.Col,
		StopDate:     location.StopDate,
	}, nil
}

// GetMyLocationRunningLast .
func (lr *LocationRepo) GetMyLocationRunningLast(ctx context.Context, userId int64) (*biz.Location, error) {
	var location Location
	if err := lr.data.db.Table("location").Where("user_id", userId).
		Where("status=?", "running").
		Order("id desc").First(&location).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	return &biz.Location{
		ID:           location.ID,
		UserId:       location.UserId,
		Status:       location.Status,
		CurrentLevel: location.CurrentLevel,
		Current:      location.Current,
		CurrentMax:   location.CurrentMax,
		Row:          location.Row,
		Col:          location.Col,
	}, nil
}

// GetMyLocationLastRunning .
func (lr *LocationRepo) GetMyLocationLastRunning(ctx context.Context, userId int64) (*biz.LocationNew, error) {
	var location LocationNew
	if err := lr.data.db.Table("location_new").Where("user_id", userId).Where("status=?", "running").Order("id desc").First(&location).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	return &biz.LocationNew{
		ID:            location.ID,
		UserId:        location.UserId,
		Status:        location.Status,
		Current:       location.Current,
		CurrentMax:    location.CurrentMax,
		CurrentMaxNew: location.CurrentMaxNew,
		StopDate:      location.StopDate,
		Top:           location.Top,
		Num:           location.Num,
		Count:         location.Count,
		TopNum:        location.TopNum,
		Usdt:          location.Usdt,
	}, nil
}

// GetLocationsNewByUserId .
func (lr *LocationRepo) GetLocationsNewByUserId(ctx context.Context, userId int64) ([]*biz.LocationNew, error) {
	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	if err := lr.data.DB(ctx).Table("location_new").
		Where("user_id=?", userId).
		Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
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
			CurrentMaxNew: location.CurrentMaxNew,
			OutRate:       location.OutRate,
			Num:           location.Num,
			StopDate:      location.StopDate,
			Usdt:          location.Usdt,
			Biw:           location.Biw,
			Top:           location.Top,
			TopNum:        location.TopNum,
			LastLevel:     location.LastLevel,
			Total:         location.Total,
			TotalTwo:      location.TotalTwo,
			TotalThree:    location.TotalThree,
		})
	}

	return res, nil
}

// UpdateLocationNewTotalSub .
func (lr *LocationRepo) UpdateLocationNewTotalSub(ctx context.Context, id int64, count int64, total int64) error {
	if 1 == count {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"total": gorm.Expr("total - ?", total)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	} else if 2 == count {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"total_two": gorm.Expr("total_two - ?", total)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	} else if 3 == count {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"total_three": gorm.Expr("total_three - ?", total)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	}

	return nil
}

// GetLocationById .
func (lr *LocationRepo) GetLocationById(ctx context.Context, id int64) (*biz.LocationNew, error) {
	var location LocationNew
	if err := lr.data.DB(ctx).Table("location_new").Where("id", id).First(&location).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	return &biz.LocationNew{
		ID:            location.ID,
		UserId:        location.UserId,
		Status:        location.Status,
		Current:       location.Current,
		CurrentMax:    location.CurrentMax,
		StopDate:      location.StopDate,
		Top:           location.Top,
		TopNum:        location.TopNum,
		Num:           location.Num,
		CurrentMaxNew: location.CurrentMaxNew,
		Count:         location.Count,
	}, nil
}

// UpdateLocationNewNew .
func (lr *LocationRepo) UpdateLocationNewNew(ctx context.Context, id int64, userId int64, status string, current int64, amountB int64, biw int64, stopDate time.Time, usdt int64) error {

	if "stop" == status {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"current": gorm.Expr("current + ?", current), "current_max_new": gorm.Expr("current_max_new + ?", amountB), "biw": gorm.Expr("biw + ?", biw), "status": "stop", "stop_date": stopDate})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}

		res = lr.data.DB(ctx).Table("user").
			Where("id=?", userId).
			Updates(map[string]interface{}{"out_rate": gorm.Expr("out_rate + ?", 1)})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}

		var reward Reward
		reward.UserId = userId
		reward.Amount = usdt
		reward.BalanceRecordId = id
		reward.Type = "out"   // 本次分红的行为类型
		reward.Reason = "out" // 给我分红的理由
		reward.ReasonLocationId = id
		var err error
		err = lr.data.DB(ctx).Table("reward").Create(&reward).Error
		if err != nil {
			return err
		}
	} else {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Where("status=?", "running").
			Updates(map[string]interface{}{"current": gorm.Expr("current + ?", current), "biw": gorm.Expr("biw + ?", biw), "status": status})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	}

	return nil
}

// GetAllLocationsNew .
func (lr *LocationRepo) GetAllLocationsNew(ctx context.Context, currentMax int64) ([]*biz.LocationNew, error) {
	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	if err := lr.data.DB(ctx).Table("location_new").Where("current_max=?", currentMax).Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.LocationNew{
			ID:            location.ID,
			UserId:        location.UserId,
			Num:           location.Num,
			Current:       location.Current,
			CurrentMax:    location.CurrentMax,
			CurrentMaxNew: location.CurrentMaxNew,
		})
	}

	return res, nil
}

// GetLocationFirst .
func (lr *LocationRepo) GetLocationFirst(ctx context.Context) (*biz.LocationNew, error) {
	var location LocationNew
	if err := lr.data.db.Table("location_new").Order("id  asc").First(&location).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	return &biz.LocationNew{
		ID:         location.ID,
		UserId:     location.UserId,
		Status:     location.Status,
		Current:    location.Current,
		CurrentMax: location.CurrentMax,
		StopDate:   location.StopDate,
		Num:        location.Num,
		Top:        location.Top,
		TopNum:     location.TopNum,
		Count:      location.Count,
	}, nil
}

// GetLocationsByTop .
func (lr *LocationRepo) GetLocationsByTop(ctx context.Context, top int64) ([]*biz.LocationNew, error) {
	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	if err := lr.data.db.Table("location_new").
		Where("top=?", top).
		Order("id asc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.LocationNew{
			ID:            location.ID,
			UserId:        location.UserId,
			Num:           location.Num,
			Current:       location.Current,
			CurrentMax:    location.CurrentMax,
			CurrentMaxNew: location.CurrentMaxNew,
			Status:        location.Status,
			StopDate:      location.StopDate,
			Count:         location.Count,
			Top:           location.Top,
			TopNum:        location.TopNum,
		})
	}

	return res, nil
}

// GetLocationsByTopTwo .
func (lr *LocationRepo) GetLocationsByTopTwo(ctx context.Context, top int64) ([]*biz.LocationNew, error) {
	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	if err := lr.data.db.Table("location_new").
		Where("top=?", top).
		Order("id asc").Limit(3).Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.LocationNew{
			ID:            location.ID,
			UserId:        location.UserId,
			Num:           location.Num,
			Current:       location.Current,
			CurrentMax:    location.CurrentMax,
			CurrentMaxNew: location.CurrentMaxNew,
			Status:        location.Status,
			StopDate:      location.StopDate,
			Count:         location.Count,
			Top:           location.Top,
			TopNum:        location.TopNum,
		})
	}

	return res, nil
}

// GetLocationsByUserId2 .
func (lr *LocationRepo) GetLocationsByUserId2(ctx context.Context, userId int64) ([]*biz.LocationNew, error) {
	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	if err := lr.data.db.Table("location_new_2").
		Where("user_id=?", userId).
		Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
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
		})
	}

	return res, nil
}

// GetAllLocationsCount .
func (lr *LocationRepo) GetAllLocationsCount(ctx context.Context, usdt int64) int64 {
	var count int64
	if err := lr.data.DB(ctx).Table("location_new").Where("usdt=?", usdt).Count(&count).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return count
		}

		return count
	}

	return count
}

// GetLocationDailyYesterday .
func (lr *LocationRepo) GetLocationDailyYesterday(ctx context.Context, day int) ([]*biz.LocationNew, error) {
	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	instance := lr.data.db.Table("location_new")

	now := time.Now().UTC().AddDate(0, 0, day)
	if now.Hour() >= 16 {
		now = now.AddDate(0, 0, 1)
	}

	// 16点之后执行
	startDate := now
	endDate := now.AddDate(0, 0, 1)
	todayStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 16, 0, 0, 0, time.UTC)
	todayEnd := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 16, 0, 0, 0, time.UTC)

	instance = instance.Where("created_at>=?", todayStart)
	instance = instance.Where("created_at<?", todayEnd)
	//测试 2024-10-28 13:36:18.048732014 +0000 UTC m=+18.455291516 2024-10-27 13:36:18.048727039 +0000 UTC
	//测试 2024-10-27 13:36:18.048727039 +0000 UTC 2024-10-28 13:36:18.048727039 +0000 UTC
	//测试 2024-10-27 16:00:00 +0000 UTC 2024-10-28 16:00:00 +0000 UTC
	//fmt.Println("测试", time.Now(), now)
	//fmt.Println("测试", startDate, endDate)
	//fmt.Println("测试", todayStart, todayEnd)
	if err := instance.Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return res, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, v := range locations {
		res = append(res, &biz.LocationNew{
			ID:         v.ID,
			UserId:     v.UserId,
			Status:     v.Status,
			Current:    v.Current,
			CurrentMax: v.CurrentMax,
			Usdt:       v.Usdt,
		})
	}

	return res, nil
}

// GetLocationsByUserId .
func (lr *LocationRepo) GetLocationsByUserId(ctx context.Context, userId int64) ([]*biz.LocationNew, error) {
	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	if err := lr.data.db.Table("location_new").
		Where("user_id=?", userId).
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

// GetLocationsStopNotUpdate .
func (lr *LocationRepo) GetLocationsStopNotUpdate(ctx context.Context) ([]*biz.Location, error) {
	var locations []*Location
	res := make([]*biz.Location, 0)
	if err := lr.data.db.Table("location").
		Where("status=?", "stop").
		Where("stop_is_update=?", 0).
		Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.Location{
			ID:           location.ID,
			UserId:       location.UserId,
			Status:       location.Status,
			CurrentLevel: location.CurrentLevel,
			Current:      location.Current,
			CurrentMax:   location.CurrentMax,
			Row:          location.Row,
			Col:          location.Col,
		})
	}

	return res, nil
}

// LockGlobalLocation .
func (lr *LocationRepo) LockGlobalLocation(ctx context.Context) (bool, error) {
	res := lr.data.DB(ctx).Where("id=? and status<=?", 1, 2).
		Table("global_lock").
		Updates(map[string]interface{}{"status": 1})

	if 0 <= res.RowsAffected {
		return true, nil
	}

	return false, res.Error

}

// UnLockGlobalLocation .
func (lr *LocationRepo) UnLockGlobalLocation(ctx context.Context) (bool, error) {
	res := lr.data.DB(ctx).Where("id=? and status=?", 1, 1).
		Table("global_lock").
		Updates(map[string]interface{}{"status": 2})

	if 0 <= res.RowsAffected {
		return true, nil
	}

	return false, res.Error
}

// LockGlobalWithdraw .
func (lr *LocationRepo) LockGlobalWithdraw(ctx context.Context) (bool, error) {
	res := lr.data.DB(ctx).Where("id=? and status>=?", 1, 2).
		Table("global_lock").
		Updates(map[string]interface{}{"status": 3})

	if 0 <= res.RowsAffected {
		return true, nil
	}

	return false, res.Error
}

// GetLockGlobalLocation .
func (lr *LocationRepo) GetLockGlobalLocation(ctx context.Context) (*biz.GlobalLock, error) {
	var globalLock GlobalLock
	if res := lr.data.DB(ctx).Where("id=?", 1).
		Table("global_lock").
		First(&globalLock); res.Error != nil {
		return nil, res.Error
	}

	return &biz.GlobalLock{
		ID:     globalLock.ID,
		Status: globalLock.Status,
	}, nil
}

// UnLockGlobalWithdraw .
func (lr *LocationRepo) UnLockGlobalWithdraw(ctx context.Context) (bool, error) {
	res := lr.data.DB(ctx).Where("id=? and status=?", 1, 3).
		Table("global_lock").
		Updates(map[string]interface{}{"status": 2})

	if 0 <= res.RowsAffected {
		return true, nil
	}

	return false, res.Error
}

// UpdateLocation .
func (lr *LocationRepo) UpdateLocation(ctx context.Context, id int64, status string, current int64, stopDate time.Time) error {

	if "stop" == status {
		res := lr.data.db.Table("location").
			Where("id=?", id).
			Updates(map[string]interface{}{"current": gorm.Expr("current + ?", current), "status": "stop", "stop_date": stopDate})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	} else {
		res := lr.data.db.Table("location").
			Where("id=?", id).
			Where("status=?", "running").
			Updates(map[string]interface{}{"current": gorm.Expr("current + ?", current), "status": status})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	}

	return nil
}

// UpdateLocationRowAndCol 事务中使用 .
func (lr *LocationRepo) UpdateLocationRowAndCol(ctx context.Context, id int64) error {

	if res := lr.data.db.Table("location").
		Where("id>?", id).
		Where("col > 1").
		Where("update_status=?", 0).
		Updates(map[string]interface{}{"col": gorm.Expr("col - ?", 1), "update_status": 1}); res.Error != nil {
		return res.Error
	}

	if res := lr.data.db.Table("location").
		Where("id>?", id).
		Where("col = 1").
		Where("update_status=?", 0).
		Updates(map[string]interface{}{"row": gorm.Expr("row - ?", 1), "col": 3, "update_status": 1}); res.Error != nil {
		return res.Error
	}

	if res := lr.data.db.Table("location").
		Where("id>?", id).
		Updates(map[string]interface{}{"update_status": 0}); res.Error != nil {
		return res.Error
	}

	if res := lr.data.db.Table("location").
		Where("id=?", id).
		Updates(map[string]interface{}{"stop_is_update": 1}); res.Error != nil {
		return res.Error
	}
	return nil
}

// GetLocationDaily .
func (lr *LocationRepo) GetLocationDaily(ctx context.Context) ([]*biz.Location, error) {
	var locations []*Location
	res := make([]*biz.Location, 0)
	instance := lr.data.db.Table("location")

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

	instance = instance.Where("created_at>=?", todayStart)
	instance = instance.Where("created_at<?", todayEnd)
	if err := instance.Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return res, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, v := range locations {
		res = append(res, &biz.Location{
			ID:           v.ID,
			UserId:       v.UserId,
			Status:       v.Status,
			CurrentLevel: v.CurrentLevel,
			Current:      v.Current,
			CurrentMax:   v.CurrentMax,
		})
	}

	return res, nil
}

// GetRewardLocationByRowOrCol .
func (lr *LocationRepo) GetRewardLocationByRowOrCol(ctx context.Context, row int64, col int64, locationRowConfig int64) ([]*biz.Location, error) {
	var (
		rowMin    int64 = 1
		rowMax    int64
		locations []*Location
	)
	if row > locationRowConfig {
		rowMin = row - locationRowConfig
	}
	rowMax = row + locationRowConfig

	if err := lr.data.db.Table("location").
		Where("status=?", "running").
		Where("row=? or (col=? and row>=? and row<=?)", row, col, rowMin, rowMax).
		Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	res := make([]*biz.Location, 0)
	for _, location := range locations {
		res = append(res, &biz.Location{
			ID:           location.ID,
			UserId:       location.UserId,
			Status:       location.Status,
			CurrentLevel: location.CurrentLevel,
			Current:      location.Current,
			CurrentMax:   location.CurrentMax,
			Row:          location.Row,
			Col:          location.Col,
			StopDate:     location.StopDate,
		})
	}

	return res, nil
}

// GetRewardLocationByIds .
func (lr *LocationRepo) GetRewardLocationByIds(ctx context.Context, ids ...int64) (map[int64]*biz.Location, error) {
	var locations []*Location
	if err := lr.data.db.Table("location").
		Where("status=?", "running").
		Where("id IN (?)", ids).
		Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	res := make(map[int64]*biz.Location, 0)
	for _, location := range locations {
		res[location.ID] = &biz.Location{
			ID:           location.ID,
			UserId:       location.UserId,
			Status:       location.Status,
			CurrentLevel: location.CurrentLevel,
			Current:      location.Current,
			CurrentMax:   location.CurrentMax,
			Row:          location.Row,
			Col:          location.Col,
		}
	}

	return res, nil
}

// GetLocationByIds .
func (lr *LocationRepo) GetLocationByIds(ctx context.Context, userIds ...int64) ([]*biz.Location, error) {
	var locations []*Location
	if err := lr.data.db.Table("location").
		Where("user_id IN (?)", userIds).
		Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	res := make([]*biz.Location, 0)
	for _, location := range locations {
		res = append(res, &biz.Location{
			ID:           location.ID,
			UserId:       location.UserId,
			Status:       location.Status,
			CurrentLevel: location.CurrentLevel,
			Current:      location.Current,
			CurrentMax:   location.CurrentMax,
			Row:          location.Row,
			Col:          location.Col,
		})
	}

	return res, nil
}

// GetLocationMapByIds .
func (lr *LocationRepo) GetLocationMapByIds(ctx context.Context, userIds ...int64) (map[int64][]*biz.Location, error) {
	var locations []*Location
	if err := lr.data.db.Table("location_new").
		Where("user_id IN (?)", userIds).
		Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	res := make(map[int64][]*biz.Location, 0)
	for _, location := range locations {
		if _, ok := res[location.UserId]; !ok {
			res[location.UserId] = make([]*biz.Location, 0)
		}

		res[location.UserId] = append(res[location.UserId], &biz.Location{
			ID:           location.ID,
			UserId:       location.UserId,
			Status:       location.Status,
			CurrentLevel: location.CurrentLevel,
			Current:      location.Current,
			CurrentMax:   location.CurrentMax,
			Row:          location.Row,
			Col:          location.Col,
			Usdt:         location.Usdt,
		})
	}

	return res, nil
}

// GetLocations .
func (lr *LocationRepo) GetLocations(ctx context.Context, b *biz.Pagination, userId int64) ([]*biz.Location, error, int64) {
	var (
		locations []*Location
		count     int64
	)
	instance := lr.data.db.Table("location").Where("status=?", "running")

	if 0 < userId {
		instance = instance.Where("user_id=?", userId)
	}

	instance = instance.Count(&count)
	if err := instance.Scopes(Paginate(b.PageNum, b.PageSize)).Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found"), 0
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error()), 0
	}

	res := make([]*biz.Location, 0)
	for _, location := range locations {
		res = append(res, &biz.Location{
			ID:           location.ID,
			UserId:       location.UserId,
			Status:       location.Status,
			CurrentLevel: location.CurrentLevel,
			Current:      location.Current,
			CurrentMax:   location.CurrentMax,
			Row:          location.Row,
			Col:          location.Col,
			CreatedAt:    location.CreatedAt,
		})
	}

	return res, nil, count
}
